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
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"golang.org/x/sys/unix"

	"github.com/sirupsen/logrus"
)

type InodeAttributes struct {
	Size  uint64
	Mtime time.Time
}

func (i InodeAttributes) Equal(other InodeAttributes) bool {
	return i.Size == other.Size && i.Mtime.Equal(other.Mtime)
}

type Inode struct {
	ID         fuseops.InodeID
	Name       *string
	fs         *Goofys
	Attributes InodeAttributes
	KnownSize  *uint64
	// It is generally safe to read `AttrTime` without locking because if some other
	// operation is modifying `AttrTime`, in most cases the reader is okay with working with
	// stale data. But Time is a struct and modifying it is not atomic. However
	// in practice (until the year 2157) we should be okay because
	// - Almost all uses of AttrTime will be about comparisions (AttrTime < x, AttrTime > x)
	// - Time object will have Time::monotonic bit set (until the year 2157) => the time
	//   comparision just compares Time::ext field
	// Ref: https://github.com/golang/go/blob/e42ae65a8507/src/time/time.go#L12:L56
	AttrTime time.Time

	mu sync.Mutex // everything below is protected by mu

	// We are not very consistent about enforcing locks for `Parent` because, the
	// parent field very very rarely changes and it is generally fine to operate on
	// stale parent informaiton
	Parent *Inode

	dir *DirInodeData

	Invalid     bool
	ImplicitDir bool

	fileHandles int32

	userMetadata map[string][]byte
	s3Metadata   map[string][]byte

	// last known etag from the cloud
	knownETag *string
	// tell the next open to invalidate page cache because the
	// file is changed. This is set when LookUp notices something
	// about this file is changed
	invalidateCache bool

	// the refcnt is an exception, it's protected by the global lock
	// Goofys.mu
	refcnt uint64
}

func NewInode(fs *Goofys, parent *Inode, name *string) (inode *Inode) {
	if strings.Contains(*name, "/") {
		fuseLog.Errorf("%v is not a valid name", *name)
	}

	inode = &Inode{
		Name:       name,
		fs:         fs,
		AttrTime:   time.Now(),
		Parent:     parent,
		s3Metadata: make(map[string][]byte),
		refcnt:     1,
	}

	return
}

func deepCopyBlobItemOputput(item *BlobItemOutput) BlobItemOutput {

	key := NilStr(item.Key)
	etag := NilStr(item.ETag)
	sc := NilStr(item.StorageClass)

	var lastmodified time.Time
	if item.LastModified != nil {
		lastmodified = *item.LastModified
	}

	return BlobItemOutput{
		Key:          &key,
		ETag:         &etag,
		LastModified: &lastmodified,
		Size:         item.Size,
		StorageClass: &sc,
	}
}

func (in *Inode) SetFromBlobItem(item *BlobItemOutput) {
	// copy item so they won't hold back references to the HTTP
	// responses and SDK objects. See discussion in
	// https://github.com/voyvodov/goofys/pull/547
	itemcopy := deepCopyBlobItemOputput(item)
	in.mu.Lock()
	defer in.mu.Unlock()

	in.Attributes.Size = itemcopy.Size
	// don't want to point to the attribute because that
	// can get updated
	size := in.Attributes.Size
	in.KnownSize = &size
	if item.LastModified != nil {
		in.Attributes.Mtime = *itemcopy.LastModified
	} else {
		in.Attributes.Mtime = in.fs.rootAttrs.Mtime
	}
	if item.ETag != nil {
		in.s3Metadata["etag"] = []byte(*itemcopy.ETag)
		in.knownETag = itemcopy.ETag
	} else {
		delete(in.s3Metadata, "etag")
	}
	if item.StorageClass != nil {
		in.s3Metadata["storage-class"] = []byte(*itemcopy.StorageClass)
	} else {
		delete(in.s3Metadata, "storage-class")
	}
	now := time.Now()
	// don't want to update time if this inode is setup to never expire
	if in.AttrTime.Before(now) {
		in.AttrTime = now
	}
}

// LOCKS_REQUIRED(inode.mu)
func (in *Inode) cloud() (cloud StorageBackend, path string) {
	var prefix string
	var dir *Inode

	if in.dir == nil {
		path = *in.Name
		dir = in.Parent
	} else {
		dir = in
	}

	for p := dir; p != nil; p = p.Parent {
		if p.dir.cloud != nil {
			cloud = p.dir.cloud
			// the error backend produces a mount.err file
			// at the root and is not aware of prefix
			_, isErr := cloud.(StorageBackendInitError)
			if !isErr {
				// we call init here instead of
				// relying on the wrapper to call init
				// because we want to return the right
				// prefix
				if c, ok := cloud.(*StorageBackendInitWrapper); ok {
					err := c.Init("")
					isErr = err != nil
				}
			}

			if !isErr {
				prefix = p.dir.mountPrefix
			}
			break
		}

		if path == "" {
			path = *p.Name
		} else if p.Parent != nil {
			// don't prepend if I am already the root node
			path = *p.Name + "/" + path
		}
	}

	if path == "" {
		path = strings.TrimRight(prefix, "/")
	} else {
		path = prefix + path
	}
	return
}

func (in *Inode) FullName() *string {
	if in.Parent == nil {
		return in.Name
	} else {
		s := in.Parent.getChildName(*in.Name)
		return &s
	}
}

func (in *Inode) touch() {
	in.Attributes.Mtime = time.Now()
}

func (in *Inode) InflateAttributes() (attr fuseops.InodeAttributes) {
	mtime := in.Attributes.Mtime
	if mtime.IsZero() {
		mtime = in.fs.rootAttrs.Mtime
	}

	attr = fuseops.InodeAttributes{
		Size:   in.Attributes.Size,
		Atime:  mtime,
		Mtime:  mtime,
		Ctime:  mtime,
		Crtime: mtime,
		Uid:    in.fs.flags.UID,
		Gid:    in.fs.flags.GID,
	}

	if in.dir != nil {
		attr.Nlink = 2
		attr.Mode = in.fs.flags.DirMode | os.ModeDir
	} else {
		attr.Nlink = 1
		attr.Mode = in.fs.flags.FileMode
	}
	return
}

func (in *Inode) logFuse(op string, args ...interface{}) {
	if fuseLog.Level >= logrus.DebugLevel {
		fuseLog.Debugln(op, in.ID, *in.FullName(), args)
	}
}

func (in *Inode) errFuse(op string, args ...interface{}) {
	fuseLog.Errorln(op, in.ID, *in.FullName(), args)
}

func (in *Inode) ToDir() {
	if in.dir == nil {
		in.Attributes = InodeAttributes{
			Size: 4096,
			// Mtime intentionally not initialized
		}
		in.dir = &DirInodeData{}
		in.KnownSize = &in.fs.rootAttrs.Size
	}
}

// Ref returns a resurrect bool
// LOCKS_REQUIRED(fs.mu)
// XXX why did I put lock required? This used to return a resurrect bool
// which no long does anything, need to look into that to see if
// that was legacy
func (in *Inode) Ref() {
	in.logFuse("Ref", in.refcnt)

	in.refcnt++
}

func (in *Inode) DeRef(n uint64) (stale bool) {
	in.logFuse("DeRef", n, in.refcnt)

	if in.refcnt < n {
		panic(fmt.Sprintf("deref %v from %v", n, in.refcnt))
	}

	in.refcnt -= n

	stale = (in.refcnt == 0)
	return
}

func (in *Inode) GetAttributes() (*fuseops.InodeAttributes, error) {
	// XXX refresh attributes
	in.logFuse("GetAttributes")
	if in.Invalid {
		return nil, fuse.ENOENT
	}
	attr := in.InflateAttributes()
	return &attr, nil
}

func (in *Inode) isDir() bool {
	return in.dir != nil
}

// LOCKS_REQUIRED(inode.mu)
func (in *Inode) fillXattrFromHead(resp *HeadBlobOutput) {
	in.userMetadata = make(map[string][]byte)

	if resp.ETag != nil {
		in.s3Metadata["etag"] = []byte(*resp.ETag)
	}
	if resp.StorageClass != nil {
		in.s3Metadata["storage-class"] = []byte(*resp.StorageClass)
	} else {
		in.s3Metadata["storage-class"] = []byte("STANDARD")
	}

	for k, v := range resp.Metadata {
		k = strings.ToLower(k)
		value, err := url.PathUnescape(*v)
		if err != nil {
			value = *v
		}
		in.userMetadata[k] = []byte(value)
	}
}

// LOCKS_REQUIRED(inode.mu)
func (in *Inode) fillXattr() (err error) {
	if !in.ImplicitDir && in.userMetadata == nil {

		fullName := *in.FullName()
		if in.isDir() {
			fullName += "/"
		}

		cloud, key := in.cloud()
		params := &HeadBlobInput{Key: key}
		resp, err := cloud.HeadBlob(params)
		if err != nil {
			err = mapAwsError(err)
			if err == fuse.ENOENT {
				err = nil
				if in.isDir() {
					in.ImplicitDir = true
				}
			}
			return err
		} else {
			in.fillXattrFromHead(resp)
		}
	}

	return
}

// LOCKS_REQUIRED(inode.mu)
func (in *Inode) getXattrMap(name string, userOnly bool) (
	meta map[string][]byte, newName string, err error) {

	cloud, _ := in.cloud()
	xattrPrefix := cloud.Capabilities().Name + "."

	if strings.HasPrefix(name, xattrPrefix) {
		if userOnly {
			return nil, "", syscall.EPERM
		}

		newName = name[len(xattrPrefix):]
		meta = in.s3Metadata
	} else if strings.HasPrefix(name, "user.") {
		err = in.fillXattr()
		if err != nil {
			return nil, "", err
		}

		newName = name[5:]
		meta = in.userMetadata
	} else {
		if userOnly {
			return nil, "", syscall.EPERM
		} else {
			return nil, "", unix.ENODATA
		}
	}

	if meta == nil {
		return nil, "", unix.ENODATA
	}

	return
}

func convertMetadata(meta map[string][]byte) (metadata map[string]*string) {
	metadata = make(map[string]*string)
	for k, v := range meta {
		k = strings.ToLower(k)
		metadata[k] = aws.String(xattrEscape(v))
	}
	return
}

// LOCKS_REQUIRED(inode.mu)
func (in *Inode) updateXattr() (err error) {
	cloud, key := in.cloud()
	_, err = cloud.CopyBlob(&CopyBlobInput{
		Source:      key,
		Destination: key,
		Size:        &in.Attributes.Size,
		ETag:        aws.String(string(in.s3Metadata["etag"])),
		Metadata:    convertMetadata(in.userMetadata),
	})
	return
}

func (in *Inode) SetXattr(name string, value []byte, flags uint32) error {
	in.logFuse("SetXattr", name)

	in.mu.Lock()
	defer in.mu.Unlock()

	meta, name, err := in.getXattrMap(name, true)
	if err != nil {
		return err
	}

	if flags != 0x0 {
		_, ok := meta[name]
		if flags == unix.XATTR_CREATE {
			if ok {
				return syscall.EEXIST
			}
		} else if flags == unix.XATTR_REPLACE {
			if !ok {
				return syscall.ENODATA
			}
		}
	}

	meta[name] = Dup(value)
	err = in.updateXattr()
	return err
}

func (in *Inode) RemoveXattr(name string) error {
	in.logFuse("RemoveXattr", name)

	in.mu.Lock()
	defer in.mu.Unlock()

	meta, name, err := in.getXattrMap(name, true)
	if err != nil {
		return err
	}

	if _, ok := meta[name]; ok {
		delete(meta, name)
		err = in.updateXattr()
		return err
	} else {
		return syscall.ENODATA
	}
}

func (in *Inode) GetXattr(name string) ([]byte, error) {
	in.logFuse("GetXattr", name)

	in.mu.Lock()
	defer in.mu.Unlock()

	meta, name, err := in.getXattrMap(name, false)
	if err != nil {
		return nil, err
	}

	value, ok := meta[name]
	if ok {
		return value, nil
	} else {
		return nil, syscall.ENODATA
	}
}

func (in *Inode) ListXattr() ([]string, error) {
	in.logFuse("ListXattr")

	in.mu.Lock()
	defer in.mu.Unlock()

	var xattrs []string

	err := in.fillXattr()
	if err != nil {
		return nil, err
	}

	cloud, _ := in.cloud()
	cloudXattrPrefix := cloud.Capabilities().Name + "."

	for k := range in.s3Metadata {
		xattrs = append(xattrs, cloudXattrPrefix+k)
	}

	for k := range in.userMetadata {
		xattrs = append(xattrs, "user."+k)
	}

	sort.Strings(xattrs)

	return xattrs, nil
}

func (in *Inode) OpenFile(metadata fuseops.OpContext) (fh *FileHandle, err error) {
	in.logFuse("OpenFile")

	in.mu.Lock()
	defer in.mu.Unlock()

	fh = NewFileHandle(in, metadata)

	atomic.AddInt32(&in.fileHandles, 1)
	return
}
