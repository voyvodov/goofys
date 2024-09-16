// Copyright 2015 - 2017 Ka-Hing Cheung
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type DirInodeData struct {
	cloud       StorageBackend
	mountPrefix string

	// these 2 refer to readdir of the Children
	lastOpenDir     *DirInodeData
	lastOpenDirIdx  int
	seqOpenDirScore uint8
	DirTime         time.Time

	Children []*Inode
}

type DirHandleEntry struct {
	Name   string
	Inode  fuseops.InodeID
	Type   fuseutil.DirentType
	Offset fuseops.DirOffset
}

// Returns true if any char in `inp` has a value < '/'.
// This should work for unicode also: unicode chars are all greater than 128.
// See TestHasCharLtSlash for examples.
func hasCharLtSlash(inp string) bool {
	for _, c := range inp {
		if c < '/' {
			return true
		}
	}
	return false
}

// Gets the name of the blob/prefix from a full cloud path.
// See TestCloudPathToName for examples.
func cloudPathToName(inp string) string {
	inp = strings.TrimRight(inp, "/")
	split := strings.Split(inp, "/")
	return split[len(split)-1]
}

// Returns true if the last prefix's name or last item's name from the given
// ListBlobsOutput has a character less than '/'
// See TestShouldFetchNextListBlobsPage for examples.
func shouldFetchNextListBlobsPage(resp *ListBlobsOutput) bool {
	if !resp.IsTruncated {
		// There is no next page.
		return false
	}
	numPrefixes := len(resp.Prefixes)
	numItems := len(resp.Items)
	if numPrefixes > 0 &&
		hasCharLtSlash(cloudPathToName(*resp.Prefixes[numPrefixes-1].Prefix)) {
		return true
	} else if numItems > 0 &&
		hasCharLtSlash(cloudPathToName(*resp.Items[numItems-1].Key)) {
		return true
	}
	return false
}

type DirHandle struct {
	inode *Inode

	mu sync.Mutex // everything below is protected by mu

	Marker        *string
	lastFromCloud *string
	done          bool
	// Time at which we started fetching child entries
	// from cloud for this handle.
	refreshStartTime time.Time
}

func NewDirHandle(inode *Inode) (dh *DirHandle) {
	dh = &DirHandle{inode: inode}
	return
}

func (in *Inode) OpenDir() (dh *DirHandle) {
	in.logFuse("OpenDir")
	var isS3 bool

	inodeP := in.Parent
	cloud, _ := in.cloud()

	// in test we sometimes set cloud to nil to ensure we are not
	// talking to the cloud
	if cloud != nil {
		_, isS3 = cloud.Delegate().(*S3Backend)
	}

	dir := in.dir
	if dir == nil {
		panic(fmt.Sprintf("%v is not a directory", in.FullName()))
	}

	if isS3 && inodeP != nil && in.fs.flags.TypeCacheTTL != 0 {
		inodeP.mu.Lock()
		defer inodeP.mu.Unlock()

		numChildren := len(inodeP.dir.Children)
		dirIdx := -1
		seqMode := false
		firstDir := false

		if inodeP.dir.lastOpenDir == nil {
			// check if we are opening the first child
			// (after . and ..)  cap the search to 1000
			// peers to bound the time. If the next dir is
			// more than 1000 away, slurping isn't going
			// to be helpful anyway
			for i := 2; i < MinInt(numChildren, 1000); i++ {
				c := inodeP.dir.Children[i]
				if c.isDir() {
					if *c.Name == *in.Name {
						dirIdx = i
						seqMode = true
						firstDir = true
					}
					break
				}
			}
		} else {
			// check if we are reading the next one as expected
			for i := inodeP.dir.lastOpenDirIdx + 1; i < MinInt(numChildren, 1000); i++ {
				c := inodeP.dir.Children[i]
				if c.isDir() {
					if *c.Name == *in.Name {
						dirIdx = i
						seqMode = true
					}
					break
				}
			}
		}

		if seqMode {
			if inodeP.dir.seqOpenDirScore < 255 {
				inodeP.dir.seqOpenDirScore++
			}
			if inodeP.dir.seqOpenDirScore == 2 {
				fuseLog.Debugf("%v in readdir mode", *inodeP.FullName())
			}
			inodeP.dir.lastOpenDir = dir
			inodeP.dir.lastOpenDirIdx = dirIdx
			if firstDir {
				// 1) if I open a/, root's score = 1
				// (a is the first dir), so make a/'s
				// count at 1 too this allows us to
				// propagate down the score for
				// depth-first search case
				wasSeqMode := dir.seqOpenDirScore >= 2
				dir.seqOpenDirScore = inodeP.dir.seqOpenDirScore
				if !wasSeqMode && dir.seqOpenDirScore >= 2 {
					fuseLog.Debugf("%v in readdir mode", *in.FullName())
				}
			}
		} else {
			inodeP.dir.seqOpenDirScore = 0
			if dirIdx == -1 {
				dirIdx = inodeP.findChildIdxUnlocked(*in.Name)
			}
			if dirIdx != -1 {
				inodeP.dir.lastOpenDir = dir
				inodeP.dir.lastOpenDirIdx = dirIdx
			}
		}
	}

	dh = NewDirHandle(in)
	return
}

func (dh *DirHandle) listObjectsSlurp(prefix string) (resp *ListBlobsOutput, err error) {
	var marker *string
	reqPrefix := prefix
	inode := dh.inode

	cloud, key := inode.cloud()

	if dh.inode.Parent != nil {
		inode = dh.inode.Parent
		var inCloud StorageBackend
		inCloud, reqPrefix = inode.cloud()
		if inCloud != cloud {
			err = fmt.Errorf("cannot slurp across cloud provider")
			return
		}

		if len(reqPrefix) != 0 {
			reqPrefix += "/"
		}
		marker = &key
		if len(*marker) != 0 {
			*marker += "/"
		}
	}

	params := &ListBlobsInput{
		Prefix:     &reqPrefix,
		StartAfter: marker,
	}

	resp, err = cloud.ListBlobs(params)
	if err != nil {
		s3Log.Errorf("ListObjects %v = %v", params, err)
		return
	}

	num := len(resp.Items)
	if num == 0 {
		return
	}

	inode.mu.Lock()
	inode.fs.mu.Lock()

	dirs := make(map[*Inode]bool)
	for _, obj := range resp.Items {
		baseName := (*obj.Key)[len(reqPrefix):]

		slash := strings.Index(baseName, "/")
		if slash != -1 {
			inode.insertSubTree(baseName, &obj, dirs)
		}
	}
	inode.fs.mu.Unlock()
	inode.mu.Unlock()

	for d, sealed := range dirs {
		if d == dh.inode {
			// never seal the current dir because that's
			// handled at upper layer
			continue
		}

		if sealed || !resp.IsTruncated {
			d.dir.DirTime = time.Now()
			d.Attributes.Mtime = d.findChildMaxTime()
		}
	}

	if resp.IsTruncated {
		obj := resp.Items[len(resp.Items)-1]
		// if we are done listing prefix, we are good
		if strings.HasPrefix(*obj.Key, prefix) {
			// if we are done with all the slashes, then we are good
			baseName := (*obj.Key)[len(prefix):]

			for _, c := range baseName {
				if c <= '/' {
					// if an entry is ex: a!b, then the
					// next entry could be a/foo, so we
					// are not done yet.
					resp = nil
					break
				}
			}
		}
	}

	// we only return this response if we are totally done with listing this dir
	if resp != nil {
		resp.IsTruncated = false
		resp.NextContinuationToken = nil
	}

	return
}

func (dh *DirHandle) listObjects(prefix string) (resp *ListBlobsOutput, err error) {
	errSlurpChan := make(chan error, 1)
	slurpChan := make(chan ListBlobsOutput, 1)
	errListChan := make(chan error, 1)
	listChan := make(chan ListBlobsOutput, 1)

	fs := dh.inode.fs

	// try to list without delimiter to see if we can slurp up
	// multiple directories
	in := dh.inode.Parent

	if dh.Marker == nil &&
		fs.flags.TypeCacheTTL != 0 &&
		(in != nil && in.dir.seqOpenDirScore >= 2) {
		go func() {
			resp, err := dh.listObjectsSlurp(prefix)
			if err != nil {
				errSlurpChan <- err
			} else if resp != nil {
				slurpChan <- *resp
			} else {
				errSlurpChan <- fuse.EINVAL
			}
		}()
	} else {
		errSlurpChan <- fuse.EINVAL
	}

	listObjectsFlat := func() {
		params := &ListBlobsInput{
			Delimiter:         aws.String("/"),
			ContinuationToken: dh.Marker,
			Prefix:            &prefix,
		}

		cloud, _ := dh.inode.cloud()

		resp, err := listBlobsSafe(cloud, params)
		if err != nil {
			errListChan <- err
		} else {
			listChan <- *resp
		}
	}

	if !fs.flags.Cheap {
		// invoke the fallback in parallel if desired
		go listObjectsFlat()
	}

	// first see if we get anything from the slurp
	select {
	case resp := <-slurpChan:
		return &resp, nil
	case <-errSlurpChan:
	}

	if fs.flags.Cheap {
		listObjectsFlat()
	}

	// if we got an error (which may mean slurp is not applicable,
	// wait for regular list
	select {
	case resp := <-listChan:
		return &resp, nil
	case err = <-errListChan:
		return
	}
}

// Sorting order of entries in directories is slightly inconsistent between goofys
// and azblob, s3. This inconsistency can be a problem if the listing involves
// multiple pagination results. Call this instead of `cloud.ListBlobs` if you are
// paginating.
//
// Problem: In s3 & azblob, prefixes are returned with '/' => the prefix "2019" is
// returned as "2019/". So the list api for these backends returns "2019/" after
// "2019-0001/" because ascii("/") > ascii("-"). This is problematic for goofys if
// "2019/" is returned in x+1'th batch and "2019-0001/" is returned in x'th; Goofys
// stores the results as they arrive in a sorted array and expects backends to return
// entries in a sorted order.
// We cant just use ordering of s3/azblob because different cloud providers have
// different sorting strategies when it involes directories. In s3 "a/" > "a-b/".
// In adlv2 it is opposite.
//
// Solution: To deal with this our solution with follows (for all backends). For
// a single call of ListBlobs, we keep requesting multiple list batches until there
// is nothing left to list or the last listed entry has all characters > "/"
// Relavant test case: TestReadDirDash
func listBlobsSafe(cloud StorageBackend, param *ListBlobsInput) (*ListBlobsOutput, error) {
	res, err := listBlobsWrapper(cloud, param)
	if err != nil {
		return nil, err
	}

	for shouldFetchNextListBlobsPage(res) {
		nextReq := &ListBlobsInput{
			// Inherit Prefix, Delimiter, MaxKeys from original request.
			Prefix:    param.Prefix,
			Delimiter: param.Delimiter,
			MaxKeys:   param.MaxKeys,
			// Get the continuation token from the result.
			ContinuationToken: res.NextContinuationToken,
		}
		nextRes, err := listBlobsWrapper(cloud, nextReq)
		if err != nil {
			return nil, err
		}

		res = &ListBlobsOutput{
			// Add new items and prefixes.
			Prefixes: append(res.Prefixes, nextRes.Prefixes...),
			Items:    append(res.Items, nextRes.Items...),
			// Inherit NextContinuationToken, IsTruncated from nextRes.
			NextContinuationToken: nextRes.NextContinuationToken,
			IsTruncated:           nextRes.IsTruncated,
			// We no longer have a single request. This is composite request. Concatenate
			// new request id to exiting.
			RequestID: res.RequestID + ", " + nextRes.RequestID,
		}
	}
	return res, nil
}

// Both in s3 and azure, it is possible that when we call listPrefix with limit=N, we might get a
// result whose size is smaller than N, but still has a continuation token. This behaviour does not
// hurt when we are listing the fuse files under a directory. But when doing dir checks i.e.
// 1) testing if the given path is a directory 2) if a given directory is empty, this can make
// goofys wrongly think a directory is empty or a given prefix is not a directory.
//
// If the backend returns less items than requested and has a continuation token, this will use the
// continuation token to fetch more items.
func listBlobsWrapper(cloud StorageBackend, param *ListBlobsInput) (*ListBlobsOutput, error) {
	targetNumElements := NilUint32(param.MaxKeys)
	ret, err := cloud.ListBlobs(param)
	if targetNumElements == 0 {
		// If MaxKeys is not specified (or is 0), we don't need any of the following special handling.
		return ret, err
	} else if err != nil {
		return nil, err
	}

	for {
		curNumElements := uint32(len(ret.Prefixes) + len(ret.Items))
		if curNumElements >= targetNumElements {
			break // We got all we want. Nothing else to do.
		} else if ret.NextContinuationToken == nil {
			break // We got all blobs under the prefix.
		}

		internalResp, err := cloud.ListBlobs(&ListBlobsInput{
			Prefix:            param.Prefix,
			Delimiter:         param.Delimiter,
			MaxKeys:           PUInt32(targetNumElements - curNumElements),
			ContinuationToken: ret.NextContinuationToken,
			// We will not set StartAfter for page requests. Only the first request might have it.
		})
		if err != nil {
			return nil, err
		}

		ret = &ListBlobsOutput{
			Prefixes:              append(ret.Prefixes, internalResp.Prefixes...),
			Items:                 append(ret.Items, internalResp.Items...),
			NextContinuationToken: internalResp.NextContinuationToken,
			IsTruncated:           internalResp.IsTruncated,
			RequestID:             internalResp.RequestID,
		}
	}
	return ret, nil
}

// ReadDir is used to list directory
// LOCKS_REQUIRED(dh.mu)
// LOCKS_EXCLUDED(dh.inode.mu)
// LOCKS_EXCLUDED(dh.inode.fs)
func (dh *DirHandle) ReadDir(offset fuseops.DirOffset) (en *DirHandleEntry, err error) {
	en, ok := dh.inode.readDirFromCache(offset)
	if ok {
		return
	}

	in := dh.inode
	fs := in.fs

	// the dir expired, so we need to fetch from the cloud. there
	// maybe static directories that we want to keep, so cloud
	// listing should not overwrite them. here's what we do:
	//
	// 1. list from cloud and add them all to the tree, remember
	//    which one we added last
	//
	// 2. serve from cache
	//
	// 3. when we serve the entry we added last, signal that next
	//    time we need to list from cloud again with continuation
	//    token
	for dh.lastFromCloud == nil && !dh.done {
		if dh.Marker == nil {
			// Marker, lastFromCloud are nil => We just started
			// refreshing this directory info from cloud.
			dh.refreshStartTime = time.Now()
		}
		dh.mu.Unlock()

		var prefix string
		_, prefix = dh.inode.cloud()
		if len(prefix) != 0 {
			prefix += "/"
		}

		resp, err := dh.listObjects(prefix)
		if err != nil {
			dh.mu.Lock()
			return nil, err
		}

		s3Log.Debug(resp)
		dh.mu.Lock()
		in.mu.Lock()
		fs.mu.Lock()

		// this is only returned for non-slurped responses
		for _, dir := range resp.Prefixes {
			// strip trailing /
			dirName := (*dir.Prefix)[0 : len(*dir.Prefix)-1]
			// strip previous prefix
			dirName = dirName[len(prefix):]
			if len(dirName) == 0 {
				continue
			}

			if inode := in.findChildUnlocked(dirName); inode != nil {
				now := time.Now()
				// don't want to update time if this
				// inode is setup to never expire
				if inode.AttrTime.Before(now) {
					inode.AttrTime = now
				}
			} else {
				inode := NewInode(fs, in, &dirName)
				inode.ToDir()
				fs.insertInode(in, inode)
				// these are fake dir entries, we will
				// realize the refcnt when lookup is
				// done
				inode.refcnt = 0
			}

			dh.lastFromCloud = &dirName
		}

		for _, obj := range resp.Items {
			if !strings.HasPrefix(*obj.Key, prefix) {
				// other slurped objects that we cached
				continue
			}

			baseName := (*obj.Key)[len(prefix):]

			slash := strings.Index(baseName, "/")
			if slash == -1 {
				if len(baseName) == 0 {
					// shouldn't happen
					continue
				}

				inode := in.findChildUnlocked(baseName)
				if inode == nil {
					inode = NewInode(fs, in, &baseName)
					// these are fake dir entries,
					// we will realize the refcnt
					// when lookup is done
					inode.refcnt = 0
					fs.insertInode(in, inode)
				}
				inode.SetFromBlobItem(&obj)
			} else {
				// this is a slurped up object which
				// was already cached
				baseName = baseName[:slash]
			}

			if dh.lastFromCloud == nil ||
				strings.Compare(*dh.lastFromCloud, baseName) < 0 {
				dh.lastFromCloud = &baseName
			}
		}

		in.mu.Unlock()
		fs.mu.Unlock()

		if resp.IsTruncated {
			dh.Marker = resp.NextContinuationToken
		} else {
			dh.Marker = nil
			dh.done = true
			break
		}
	}

	in.mu.Lock()
	defer in.mu.Unlock()

	// Find the first non-stale child inode with offset >=
	// `offset`. A stale inode is one that existed before the
	// first ListBlobs for this dir handle, but is not being
	// written to (ie: not a new file)
	var child *Inode
	for int(offset) < len(in.dir.Children) {
		// Note on locking: See comments at Inode::AttrTime, Inode::Parent.
		childTmp := in.dir.Children[offset]
		if atomic.LoadInt32(&childTmp.fileHandles) == 0 &&
			childTmp.AttrTime.Before(dh.refreshStartTime) {
			// childTmp.AttrTime < dh.refreshStartTime => the child entry was not
			// updated from cloud by this dir Handle.
			// So this is a stale entry that should be removed.
			childTmp.Parent = nil
			in.removeChildUnlocked(childTmp)
		} else {
			// Found a non-stale child inode.
			child = childTmp
			break
		}
	}

	if child == nil {
		// we've reached the end
		in.dir.DirTime = time.Now()
		in.Attributes.Mtime = in.findChildMaxTime()
		return nil, nil
	}

	en = &DirHandleEntry{
		Name:   *child.Name,
		Inode:  child.ID,
		Offset: fuseops.DirOffset(offset) + 1,
	}
	if child.isDir() {
		en.Type = fuseutil.DT_Directory
	} else {
		en.Type = fuseutil.DT_File
	}

	if dh.lastFromCloud != nil && en.Name == *dh.lastFromCloud {
		dh.lastFromCloud = nil
	}
	return en, nil
}

func (dh *DirHandle) CloseDir() error {
	return nil
}

// prefix and newPrefix should include the trailing /
// return all the renamed objects
func (in *Inode) renameChildren(cloud StorageBackend, prefix string,
	newParent *Inode, newPrefix string) (err error) {

	var copied []string
	var res *ListBlobsOutput

	for {
		param := ListBlobsInput{
			Prefix: &prefix,
		}
		if res != nil {
			param.ContinuationToken = res.NextContinuationToken
		}

		// No need to call listBlobsSafe here because we are reading the results directly
		// unlike ReadDir which reads the results and stores it in dir object.
		res, err = cloud.ListBlobs(&param)
		if err != nil {
			return
		}

		if len(res.Items) == 0 {
			return
		}

		if copied == nil {
			copied = make([]string, 0, len(res.Items))
		}

		// after the server side copy, we want to delete all the files
		// using multi-delete, which is capped to 1000 on aws. If we
		// are going to make an arbitrary limit that sounds like a
		// good one (and we want to have an arbitrary limit because we
		// don't want to rename a million objects here)
		total := len(copied) + len(res.Items)
		if total > 1000 || total == 1000 && res.IsTruncated {
			return syscall.E2BIG
		}

		// say dir is "/a/dir" and it has "1", "2", "3", and we are
		// moving it to "/b/" items will be a/dir/1, a/dir/2, a/dir/3,
		// and we will copy them to b/1, b/2, b/3 respectively
		for _, i := range res.Items {
			key := (*i.Key)[len(prefix):]

			// TODO: coordinate with underlining copy and do this in parallel
			_, err = cloud.CopyBlob(&CopyBlobInput{
				Source:       *i.Key,
				Destination:  newPrefix + key,
				Size:         &i.Size,
				ETag:         i.ETag,
				StorageClass: i.StorageClass,
			})
			if err != nil {
				return err
			}

			copied = append(copied, *i.Key)
		}

		if !res.IsTruncated {
			break
		}
	}

	s3Log.Debugf("rename copied %v", copied)
	_, err = cloud.DeleteBlobs(&DeleteBlobsInput{Items: copied})
	return err
}

// Recursively resets the DirTime for child directories.
// ACQUIRES_LOCK(inode.mu)
func (in *Inode) resetDirTimeRec() {
	in.mu.Lock()
	if in.dir == nil {
		in.mu.Unlock()
		return
	}
	in.dir.DirTime = time.Time{}
	// Make a copy of the child nodes before giving up the lock.
	// This protects us from any addition/removal of child nodes
	// under this node.
	children := make([]*Inode, len(in.dir.Children))
	copy(children, in.dir.Children)
	in.mu.Unlock()
	for _, child := range children {
		child.resetDirTimeRec()
	}
}

// ResetForUnmount resets the Inode as part of unmounting a storage backend
// mounted at the given inode.
// ACQUIRES_LOCK(inode.mu)
func (in *Inode) ResetForUnmount() {
	if in.dir == nil {
		panic(fmt.Sprintf("ResetForUnmount called on a non-directory. name:%v",
			in.Name))
	}

	in.mu.Lock()
	// First reset the cloud info for this directory. After that, any read and
	// write operations under this directory will not know about this cloud.
	in.dir.cloud = nil
	in.dir.mountPrefix = ""

	// Clear metadata.
	// Set the metadata values to nil instead of deleting them so that
	// we know to fetch them again next time instead of thinking there's
	// no metadata
	in.userMetadata = nil
	in.s3Metadata = nil
	in.Attributes = InodeAttributes{}
	in.Invalid, in.ImplicitDir = false, false
	in.mu.Unlock()
	// Reset DirTime for recursively for this node and all its child nodes.
	// Note: resetDirTimeRec should be called without holding the lock.
	in.resetDirTimeRec()

}

func (in *Inode) findPath(path string) (inode *Inode) {
	dir := in

	for dir != nil {
		if !dir.isDir() {
			return nil
		}

		idx := strings.Index(path, "/")
		if idx == -1 {
			return dir.findChild(path)
		}
		dirName := path[0:idx]
		path = path[idx+1:]

		dir = dir.findChild(dirName)
	}

	return nil
}

func (in *Inode) findChild(name string) (inode *Inode) {
	in.mu.Lock()
	defer in.mu.Unlock()

	inode = in.findChildUnlocked(name)
	return
}

func (in *Inode) findInodeFunc(name string) func(i int) bool {
	return func(i int) bool {
		return (*in.dir.Children[i].Name) >= name
	}
}

func (in *Inode) findChildUnlocked(name string) (inode *Inode) {
	l := len(in.dir.Children)
	if l == 0 {
		return
	}
	i := sort.Search(l, in.findInodeFunc(name))
	if i < l {
		// found
		if *in.dir.Children[i].Name == name {
			inode = in.dir.Children[i]
		}
	}
	return
}

func (in *Inode) findChildIdxUnlocked(name string) int {
	l := len(in.dir.Children)
	if l == 0 {
		return -1
	}
	i := sort.Search(l, in.findInodeFunc(name))
	if i < l && *in.dir.Children[i].Name == name {
		return i
	}
	return -1
}

func (in *Inode) removeChildUnlocked(inode *Inode) {
	l := len(in.dir.Children)
	if l == 0 {
		return
	}
	i := sort.Search(l, in.findInodeFunc(*inode.Name))
	if i >= l || *in.dir.Children[i].Name != *inode.Name {
		panic(fmt.Sprintf("%v.removeName(%v) but child not found: %v",
			*in.FullName(), *inode.Name, i))
	}

	copy(in.dir.Children[i:], in.dir.Children[i+1:])
	in.dir.Children[l-1] = nil
	in.dir.Children = in.dir.Children[:l-1]

	if cap(in.dir.Children)-len(in.dir.Children) > 20 {
		tmp := make([]*Inode, len(in.dir.Children))
		copy(tmp, in.dir.Children)
		in.dir.Children = tmp
	}
}

func (in *Inode) removeChild(inode *Inode) {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.removeChildUnlocked(inode)
}

func (in *Inode) insertChild(inode *Inode) {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.insertChildUnlocked(inode)
}

func (in *Inode) insertChildUnlocked(inode *Inode) {
	l := len(in.dir.Children)
	if l == 0 {
		in.dir.Children = []*Inode{inode}
		return
	}

	i := sort.Search(l, in.findInodeFunc(*inode.Name))
	if i == l {
		// not found = new value is the biggest
		in.dir.Children = append(in.dir.Children, inode)
	} else {
		if *in.dir.Children[i].Name == *inode.Name {
			panic(fmt.Sprintf("double insert of %v", in.getChildName(*inode.Name)))
		}

		in.dir.Children = append(in.dir.Children, nil)
		copy(in.dir.Children[i+1:], in.dir.Children[i:])
		in.dir.Children[i] = inode
	}
}

func (in *Inode) LookUp(name string) (inode *Inode, err error) {
	in.logFuse("Inode.LookUp", name)

	inode, err = in.LookUpInodeMaybeDir(name, in.getChildName(name))
	if err != nil {
		return nil, err
	}

	return
}

func (in *Inode) getChildName(name string) string {
	if in.ID == fuseops.RootInodeID {
		return name
	} else {
		return fmt.Sprintf("%v/%v", *in.FullName(), name)
	}
}

func (in *Inode) Unlink(name string) (err error) {
	in.logFuse("Unlink", name)

	cloud, key := in.cloud()
	key = appendChildName(key, name)

	_, err = cloud.DeleteBlob(&DeleteBlobInput{
		Key: key,
	})
	if err == fuse.ENOENT {
		// this might have been deleted out of band
		err = nil
	}
	if err != nil {
		return
	}

	in.mu.Lock()
	defer in.mu.Unlock()

	inode := in.findChildUnlocked(name)
	if inode != nil {
		in.removeChildUnlocked(inode)
		inode.Parent = nil
	}

	return
}

func (in *Inode) Create(
	name string, metadata fuseops.OpContext) (inode *Inode, fh *FileHandle) {

	in.logFuse("Create", name)

	fs := in.fs

	in.mu.Lock()
	defer in.mu.Unlock()

	now := time.Now()
	inode = NewInode(fs, in, &name)
	inode.Attributes = InodeAttributes{
		Size:  0,
		Mtime: now,
	}

	fh = NewFileHandle(inode, metadata)
	fh.poolHandle = fs.bufferPool
	fh.dirty = true
	inode.fileHandles = 1

	in.touch()

	return
}

func (in *Inode) MkDir(
	name string) (inode *Inode, err error) {

	in.logFuse("MkDir", name)

	fs := in.fs

	cloud, key := in.cloud()
	key = appendChildName(key, name)
	if !cloud.Capabilities().DirBlob {
		key += "/"
	}
	params := &PutBlobInput{
		Key:     key,
		Body:    nil,
		DirBlob: true,
	}

	_, err = cloud.PutBlob(params)
	if err != nil {
		return
	}

	in.mu.Lock()
	defer in.mu.Unlock()

	inode = NewInode(fs, in, &name)
	inode.ToDir()
	inode.touch()
	if in.Attributes.Mtime.Before(inode.Attributes.Mtime) {
		in.Attributes.Mtime = inode.Attributes.Mtime
	}

	return
}

func appendChildName(in, child string) string {
	if len(in) != 0 {
		in += "/"
	}
	return in + child
}

func (in *Inode) isEmptyDir(fs *Goofys, name string) (isDir bool, err error) {
	cloud, key := in.cloud()
	key = appendChildName(key, name) + "/"

	resp, err := listBlobsWrapper(cloud, &ListBlobsInput{
		Delimiter: aws.String("/"),
		MaxKeys:   PUInt32(2),
		Prefix:    &key,
	})
	if err != nil {
		return false, mapAwsError(err)
	}

	if len(resp.Prefixes) > 0 || len(resp.Items) > 1 {
		err = fuse.ENOTEMPTY
		isDir = true
		return
	}

	if len(resp.Items) == 1 {
		isDir = true

		if *resp.Items[0].Key != key {
			err = fuse.ENOTEMPTY
		}
	}

	return
}

func (in *Inode) RmDir(name string) (err error) {
	in.logFuse("Rmdir", name)

	isDir, err := in.isEmptyDir(in.fs, name)
	if err != nil {
		return
	}
	// if this was an implicit dir, isEmptyDir would have returned
	// isDir = false
	if isDir {
		cloud, key := in.cloud()
		key = appendChildName(key, name) + "/"

		params := DeleteBlobInput{
			Key: key,
		}

		_, err = cloud.DeleteBlob(&params)
		if err != nil {
			return
		}
	}

	// we know this entry is gone
	in.mu.Lock()
	defer in.mu.Unlock()

	inode := in.findChildUnlocked(name)
	if inode != nil {
		in.removeChildUnlocked(inode)
		inode.Parent = nil
	}

	return
}

// Rename can be used to rename files/folders
// semantic of rename:
// rename("any", "not_exists") = ok
// rename("file1", "file2") = ok
// rename("empty_dir1", "empty_dir2") = ok
// rename("nonempty_dir1", "empty_dir2") = ok
// rename("nonempty_dir1", "nonempty_dir2") = ENOTEMPTY
// rename("file", "dir") = EISDIR
// rename("dir", "file") = ENOTDIR
func (in *Inode) Rename(from string, newParent *Inode, to string) (err error) {
	in.logFuse("Rename", from, newParent.getChildName(to))

	fromCloud, fromPath := in.cloud()
	toCloud, toPath := newParent.cloud()
	if fromCloud != toCloud {
		// cannot rename across cloud backend
		err = fuse.EINVAL
		return
	}

	fromFullName := appendChildName(fromPath, from)
	fs := in.fs

	var size *uint64
	var fromIsDir bool
	var toIsDir bool
	var renameChildren bool

	fromIsDir, err = in.isEmptyDir(fs, from)
	if err != nil {
		if err == fuse.ENOTEMPTY {
			renameChildren = true
		} else {
			return
		}
	}

	toFullName := appendChildName(toPath, to)

	toIsDir, err = in.isEmptyDir(fs, to)
	if err != nil {
		return
	}

	if fromIsDir && !toIsDir {
		_, err = fromCloud.HeadBlob(&HeadBlobInput{
			Key: toFullName,
		})
		if err == nil {
			return fuse.ENOTDIR
		} else {
			err = mapAwsError(err)
			if err != fuse.ENOENT {
				return
			}
		}
	} else if !fromIsDir && toIsDir {
		return syscall.EISDIR
	}

	if fromIsDir {
		fromFullName += "/"
		toFullName += "/"
		size = PUInt64(0)
	}

	if renameChildren && !fromCloud.Capabilities().DirBlob {
		err = in.renameChildren(fromCloud, fromFullName,
			newParent, toFullName)
		if err != nil {
			return
		}
	} else {
		err = in.renameObject(fs, size, fromFullName, toFullName)
	}
	return
}

func (in *Inode) renameObject(fs *Goofys, size *uint64, fromFullName string, toFullName string) (err error) {
	cloud, _ := in.cloud()

	_, err = cloud.RenameBlob(&RenameBlobInput{
		Source:      fromFullName,
		Destination: toFullName,
	})
	if err == nil || err != syscall.ENOTSUP {
		return
	}

	_, err = cloud.CopyBlob(&CopyBlobInput{
		Source:      fromFullName,
		Destination: toFullName,
		Size:        size,
	})
	if err != nil {
		return
	}

	_, err = cloud.DeleteBlob(&DeleteBlobInput{
		Key: fromFullName,
	})
	if err != nil {
		return
	}
	s3Log.Debugf("Deleted %v", fromFullName)

	return
}

// if I had seen a/ and a/b, and now I get a/c, that means a/b is
// done, but not a/
func (in *Inode) isParentOf(inode *Inode) bool {
	return inode.Parent != nil && (in == inode.Parent || in.isParentOf(inode.Parent))
}

func sealPastDirs(dirs map[*Inode]bool, d *Inode) {
	for p, sealed := range dirs {
		if p != d && !sealed && !p.isParentOf(d) {
			dirs[p] = true
		}
	}
	// I just read something in d, obviously it's not done yet
	dirs[d] = false
}

// LOCKS_REQUIRED(fs.mu)
// LOCKS_REQUIRED(in.mu)
// LOCKS_REQUIRED(in.fs.mu)
func (in *Inode) insertSubTree(path string, obj *BlobItemOutput, dirs map[*Inode]bool) {
	fs := in.fs
	slash := strings.Index(path, "/")
	if slash == -1 {
		inode := in.findChildUnlocked(path)
		if inode == nil {
			inode = NewInode(fs, in, &path)
			inode.refcnt = 0
			fs.insertInode(in, inode)
			inode.SetFromBlobItem(obj)
		} else {
			// our locking order is most specific lock
			// first, ie: lock a/b before a/. But here we
			// already have a/ and also global lock. For
			// new inode we don't care about that
			// violation because no one else will take
			// that lock anyway
			fs.mu.Unlock()
			in.mu.Unlock()
			inode.SetFromBlobItem(obj)
			in.mu.Lock()
			fs.mu.Lock()
		}
		sealPastDirs(dirs, in)
	} else {
		dir := path[:slash]
		path = path[slash+1:]

		if len(path) == 0 {
			inode := in.findChildUnlocked(dir)
			if inode == nil {
				inode = NewInode(fs, in, &dir)
				inode.ToDir()
				inode.refcnt = 0
				fs.insertInode(in, inode)
				inode.SetFromBlobItem(obj)
			} else if !inode.isDir() {
				inode.ToDir()
				fs.addDotAndDotDot(inode)
			} else {
				fs.mu.Unlock()
				in.mu.Unlock()
				inode.SetFromBlobItem(obj)
				in.mu.Lock()
				fs.mu.Lock()
			}
			sealPastDirs(dirs, inode)
		} else {
			// ensure that the potentially implicit dir is added
			inode := in.findChildUnlocked(dir)
			if inode == nil {
				inode = NewInode(fs, in, &dir)
				inode.ToDir()
				inode.refcnt = 0
				fs.insertInode(in, inode)
			} else if !inode.isDir() {
				inode.ToDir()
				fs.addDotAndDotDot(inode)
			}
			now := time.Now()
			if inode.AttrTime.Before(now) {
				inode.AttrTime = now
			}

			// mark this dir but don't seal anything else
			// until we get to the leaf
			dirs[inode] = false

			fs.mu.Unlock()
			in.mu.Unlock()
			inode.mu.Lock()
			fs.mu.Lock()
			inode.insertSubTree(path, obj, dirs)
			inode.mu.Unlock()
			fs.mu.Unlock()
			in.mu.Lock()
			fs.mu.Lock()
		}
	}
}

func (in *Inode) findChildMaxTime() time.Time {
	maxTime := in.Attributes.Mtime

	for i, c := range in.dir.Children {
		if i < 2 {
			// skip . and ..
			continue
		}
		if c.Attributes.Mtime.After(maxTime) {
			maxTime = c.Attributes.Mtime
		}
	}

	return maxTime
}

func (in *Inode) readDirFromCache(offset fuseops.DirOffset) (en *DirHandleEntry, ok bool) {
	in.mu.Lock()
	defer in.mu.Unlock()

	if in.dir == nil {
		panic(*in.FullName())
	}
	if !expired(in.dir.DirTime, in.fs.flags.TypeCacheTTL) {
		ok = true

		if int(offset) >= len(in.dir.Children) {
			return
		}
		child := in.dir.Children[offset]

		en = &DirHandleEntry{
			Name:   *child.Name,
			Inode:  child.ID,
			Offset: offset + 1,
		}
		if child.isDir() {
			en.Type = fuseutil.DT_Directory
		} else {
			en.Type = fuseutil.DT_File
		}

	}
	return
}

func (in *Inode) LookUpInodeNotDir(name string, c chan HeadBlobOutput, errc chan error) {
	cloud, key := in.cloud()
	key = appendChildName(key, name)
	params := &HeadBlobInput{Key: key}
	resp, err := cloud.HeadBlob(params)
	if err != nil {
		errc <- mapAwsError(err)
		return
	}

	s3Log.Debug(resp)
	c <- *resp
}

func (in *Inode) LookUpInodeDir(name string, c chan ListBlobsOutput, errc chan error) {
	cloud, key := in.cloud()
	key = appendChildName(key, name) + "/"

	resp, err := listBlobsWrapper(cloud, &ListBlobsInput{
		Delimiter: aws.String("/"),
		// Ideally one result should be sufficient. But when azure hierarchical
		// namespaces are enabled, azblob returns "a" when we list blobs under "a/".
		// In such cases we remove "a" from the result. So request for 2 blobs.
		MaxKeys: PUInt32(1),
		Prefix:  &key,
	})

	if err != nil {
		errc <- err
		return
	}

	s3Log.Debug(resp)
	c <- *resp
}

// LookUpInodeMaybeDir returned inode has nil Id
func (in *Inode) LookUpInodeMaybeDir(name string, fullName string) (inode *Inode, err error) {
	errObjectChan := make(chan error, 1)
	objectChan := make(chan HeadBlobOutput, 2)
	errDirBlobChan := make(chan error, 1)
	var errDirChan chan error
	var dirChan chan ListBlobsOutput

	checking := 3
	var checkErr [3]error

	cloud, _ := in.cloud()
	if cloud == nil {
		panic("s3 disabled")
	}

	go in.LookUpInodeNotDir(name, objectChan, errObjectChan)
	if !cloud.Capabilities().DirBlob && !in.fs.flags.Cheap {
		go in.LookUpInodeNotDir(name+"/", objectChan, errDirBlobChan)
		if !in.fs.flags.ExplicitDir {
			errDirChan = make(chan error, 1)
			dirChan = make(chan ListBlobsOutput, 1)
			go in.LookUpInodeDir(name, dirChan, errDirChan)
		}
	}

	for {
		select {
		case resp := <-objectChan:
			err = nil
			inode = NewInode(in.fs, in, &name)
			if !resp.IsDirBlob {
				// XXX/TODO if both object and object/ exists, return dir
				inode.SetFromBlobItem(&resp.BlobItemOutput)
			} else {
				inode.ToDir()
				if resp.LastModified != nil {
					inode.Attributes.Mtime = *resp.LastModified
				}
			}
			inode.fillXattrFromHead(&resp)
			return
		case err = <-errObjectChan:
			checking--
			checkErr[0] = err
			s3Log.Debugf("HEAD %v = %v", fullName, err)
		case resp := <-dirChan:
			err = nil
			if len(resp.Prefixes) != 0 || len(resp.Items) != 0 {
				inode = NewInode(in.fs, in, &name)
				inode.ToDir()
				if len(resp.Items) != 0 && *resp.Items[0].Key == name+"/" {
					// it's actually a dir blob
					entry := resp.Items[0]
					if entry.ETag != nil {
						inode.s3Metadata["etag"] = []byte(*entry.ETag)
					}
					if entry.StorageClass != nil {
						inode.s3Metadata["storage-class"] = []byte(*entry.StorageClass)
					}

				}
				// if cheap is not on, the dir blob
				// could exist but this returned first
				if inode.fs.flags.Cheap {
					inode.ImplicitDir = true
				}
				return
			} else {
				checkErr[2] = fuse.ENOENT
				checking--
			}
		case err = <-errDirChan:
			checking--
			checkErr[2] = err
			s3Log.Debugf("LIST %v/ = %v", fullName, err)
		case err = <-errDirBlobChan:
			checking--
			checkErr[1] = err
			s3Log.Debugf("HEAD %v/ = %v", fullName, err)
		}

		if cloud.Capabilities().DirBlob {
			return
		}

		switch checking {
		case 2:
			if in.fs.flags.Cheap {
				go in.LookUpInodeNotDir(name+"/", objectChan, errDirBlobChan)
			}
		case 1:
			if in.fs.flags.ExplicitDir {
				checkErr[2] = fuse.ENOENT
				goto doneCase
			} else if in.fs.flags.Cheap {
				errDirChan = make(chan error, 1)
				dirChan = make(chan ListBlobsOutput, 1)
				go in.LookUpInodeDir(name, dirChan, errDirChan)
			}
			break
		doneCase:
			fallthrough
		case 0:
			for _, e := range checkErr {
				if e != fuse.ENOENT {
					err = e
					return
				}
			}

			err = fuse.ENOENT
			return
		}
	}
}
