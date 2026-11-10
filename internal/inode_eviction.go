package internal

import (
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/jacobsa/fuse/fuseops"
)

func (fs *Goofys) startInodeEviction(ctx context.Context) {
	if fs.flags.InodeCacheTTL <= 0 {
		return
	}
	ctx, fs.evictionCancel = context.WithCancel(ctx)
	fs.evictionDone = make(chan struct{})
	interval := fs.flags.InodeCacheTTL
	if interval > time.Minute {
		interval = time.Minute
	}
	if interval < time.Second {
		interval = time.Second
	}
	go func() {
		defer close(fs.evictionDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				fs.evictInodes(now)
			}
		}
	}()
}

// Destroy is called by the FUSE server on unmount. Canceling the constructor's
// context also stops eviction. Direct NewGoofys callers must call Destroy.
func (fs *Goofys) Destroy() {
	if fs.evictionCancel != nil {
		fs.evictionCancel()
		<-fs.evictionDone
	}
}

type inodeEvictionCandidate struct {
	inode  *Inode
	parent *Inode
	depth  int
}

// evictInodes removes idle files and empty directories, deepest first. It does
// not query storage: objects that still exist are rediscovered by later reads.
func (fs *Goofys) evictInodes(now time.Time) int {
	if fs.flags.InodeCacheTTL <= 0 {
		return 0
	}
	cutoff := now.Add(-fs.flags.InodeCacheTTL)
	var candidates []inodeEvictionCandidate
	fs.mu.RLock()
	if fs.activeSlurps != 0 {
		fs.mu.RUnlock()
		return 0
	}
	for id, inode := range fs.inodes {
		if id == fuseops.RootInodeID || inode.refcnt != 0 || inode.lastUsed.After(cutoff) {
			continue
		}
		candidate := inodeEvictionCandidate{inode: inode, parent: inode.Parent}
		for p := inode.Parent; p != nil; p = p.Parent {
			candidate.depth++
		}
		candidates = append(candidates, candidate)
	}
	fs.mu.RUnlock()
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].depth > candidates[j].depth })
	collected := 0
	for _, candidate := range candidates {
		if fs.evictInode(candidate, cutoff, now) {
			collected++
		}
	}
	return collected
}

func (fs *Goofys) evictInode(candidate inodeEvictionCandidate, cutoff, now time.Time) bool {
	inode, parent := candidate.inode, candidate.parent
	if parent != nil {
		if !parent.mu.TryLock() {
			return false
		}
		defer parent.mu.Unlock()
	}
	// Rename may already hold this inode while waiting for another directory.
	// Skip busy entries rather than introduce a new inode lock ordering.
	if !inode.mu.TryLock() {
		return false
	}
	defer inode.mu.Unlock()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.activeSlurps != 0 || inode.ID == fuseops.RootInodeID || fs.inodes[inode.ID] != inode ||
		inode.Parent != parent || inode.refcnt != 0 || inode.lastUsed.After(cutoff) ||
		inode.AttrTime.After(now) || atomic.LoadInt32(&inode.fileHandles) != 0 {
		return false
	}
	for _, fh := range fs.fileHandles {
		if fh.inode == inode {
			return false
		}
	}
	for _, dh := range fs.dirHandles {
		if dh.inode == inode || dh.inode == parent {
			return false
		}
	}
	if inode.isDir() {
		for _, child := range inode.dir.Children {
			if *child.Name != "." && *child.Name != ".." {
				return false
			}
		}
	}
	if parent != nil {
		if parent.findChildUnlocked(*inode.Name) != inode {
			return false
		}
		parent.removeChildUnlocked(inode)
		parent.dir.DirTime = time.Time{}
		if inode.dir != nil && parent.dir.lastOpenDir == inode.dir {
			parent.dir.lastOpenDir = nil
			parent.dir.lastOpenDirIdx = 0
			parent.dir.seqOpenDirScore = 0
		}
	}
	inode.Parent = nil
	delete(fs.inodes, inode.ID)
	fs.forgotCnt++
	return true
}
