package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/urfave/cli"
)

type evictionBackend struct {
	StorageBackend
	head func(*HeadBlobInput) (*HeadBlobOutput, error)
}

func (b *evictionBackend) Capabilities() *Capabilities { return &Capabilities{DirBlob: true} }
func (b *evictionBackend) Delegate() interface{}       { return b }
func (b *evictionBackend) HeadBlob(in *HeadBlobInput) (*HeadBlobOutput, error) {
	return b.head(in)
}

func idleTestInode(fs *Goofys, parent *Inode, name string, dir bool, refs uint64) *Inode {
	inode := insertTestInode(fs, parent, name, dir, refs)
	inode.lastUsed = time.Now().Add(-time.Hour)
	inode.AttrTime = inode.lastUsed
	return inode
}

func TestInodeEvictionExternalDeletionAndRediscovery(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprintf("objectPresent=%v", present), func(t *testing.T) {
			fs := newTestFS()
			fs.flags.InodeCacheTTL = time.Minute
			root := fs.inodes[fuseops.RootInodeID]
			root.dir.DirTime = time.Now()
			old := idleTestInode(fs, root, "file", false, 0)
			root.dir.cloud = &evictionBackend{head: func(in *HeadBlobInput) (*HeadBlobOutput, error) {
				if !present {
					return nil, fuse.ENOENT
				}
				return &HeadBlobOutput{BlobItemOutput: BlobItemOutput{Key: &in.Key}}, nil
			}}
			if n := fs.evictInodes(time.Now()); n != 1 {
				t.Fatalf("evicted %d inodes, want 1", n)
			}
			if fs.inodes[old.ID] != nil || root.findChild("file") != nil || !root.dir.DirTime.IsZero() {
				t.Fatal("eviction left stale map, child or directory cache state")
			}
			op := &fuseops.LookUpInodeOp{Parent: root.ID, Name: "file"}
			err := fs.LookUpInode(context.Background(), op)
			if !present {
				if err != fuse.ENOENT {
					t.Fatalf("lookup of deleted object returned %v, want ENOENT", err)
				}
			} else if err != nil || op.Entry.Child == old.ID || fs.inodes[op.Entry.Child] == nil {
				t.Fatalf("existing object was not rediscovered: entry=%v err=%v", op.Entry.Child, err)
			}
		})
	}
}

func TestInodeEvictionBottomUp(t *testing.T) {
	fs := newTestFS()
	fs.flags.InodeCacheTTL = time.Minute
	root := fs.inodes[fuseops.RootInodeID]
	dir := idleTestInode(fs, root, "dir", true, 0)
	sub := idleTestInode(fs, dir, "sub", true, 0)
	idleTestInode(fs, sub, "file", false, 0)
	root.dir.lastOpenDir = dir.dir
	if n := fs.evictInodes(time.Now()); n != 3 || len(fs.inodes) != 1 {
		t.Fatalf("evicted %d inodes, %d remain; want 3 and root", n, len(fs.inodes))
	}
	if root.dir.lastOpenDir != nil {
		t.Fatal("directory scan heuristic retained the evicted directory")
	}
}

func TestInodeEvictionProtectsInUseEntries(t *testing.T) {
	for _, protect := range []string{"reference", "fileCounter", "fileHandle", "directoryHandle", "parentHandle", "recent", "static", "nonempty", "busy", "busyParent", "slurp"} {
		t.Run(protect, func(t *testing.T) {
			fs := newTestFS()
			fs.flags.InodeCacheTTL = time.Minute
			root := fs.inodes[fuseops.RootInodeID]
			inode := idleTestInode(fs, root, "entry", protect == "directoryHandle" || protect == "nonempty", 0)
			switch protect {
			case "reference":
				inode.refcnt = 1
			case "fileCounter":
				inode.fileHandles = 1
			case "fileHandle":
				fs.fileHandles = map[fuseops.HandleID]*FileHandle{1: {inode: inode}}
			case "directoryHandle":
				fs.dirHandles = map[fuseops.HandleID]*DirHandle{1: NewDirHandle(inode)}
			case "parentHandle":
				fs.dirHandles = map[fuseops.HandleID]*DirHandle{1: NewDirHandle(root)}
			case "recent":
				inode.lastUsed = time.Now()
			case "static":
				inode.AttrTime = timeMax
			case "nonempty":
				idleTestInode(fs, inode, "referenced", false, 1)
			case "busy":
				inode.mu.Lock()
				defer inode.mu.Unlock()
			case "slurp":
				fs.activeSlurps = 1
			case "busyParent":
				root.mu.Lock()
				defer root.mu.Unlock()
			}
			if n := fs.evictInodes(time.Now()); n != 0 || fs.inodes[inode.ID] != inode {
				t.Fatalf("eviction removed protected entry (%d collected)", n)
			}
		})
	}
}

func TestInodeEvictionRevalidatesCandidate(t *testing.T) {
	for _, change := range []string{"parent", "reference", "touch", "identity", "slurp"} {
		t.Run(change, func(t *testing.T) {
			fs := newTestFS()
			root := fs.inodes[fuseops.RootInodeID]
			inode := idleTestInode(fs, root, "file", false, 0)
			candidate := inodeEvictionCandidate{inode: inode, parent: root}
			switch change {
			case "parent":
				root.removeChild(inode)
				inode.Parent = nil
			case "reference":
				inode.Ref()
			case "touch":
				inode.lastUsed = time.Now()
			case "identity":
				other := NewInode(fs, root, PString("other"))
				other.ID = inode.ID
				fs.inodes[inode.ID] = other
			case "slurp":
				fs.activeSlurps = 1
			}
			now := time.Now()
			if fs.evictInode(candidate, now.Add(-time.Minute), now) || fs.inodes[inode.ID] == nil {
				t.Fatal("eviction did not reject a stale candidate")
			}
		})
	}
}

func TestInodeEvictionDirectoryReadsTouchCache(t *testing.T) {
	fs := newTestFS()
	fs.flags.InodeCacheTTL = time.Minute
	fs.flags.TypeCacheTTL = time.Hour
	root := fs.inodes[fuseops.RootInodeID]
	root.dir.DirTime = time.Now()
	inode := idleTestInode(fs, root, "file", false, 0)
	entry, ok := root.readDirFromCache(2)
	if !ok || entry == nil || entry.Inode != inode.ID {
		t.Fatal("did not serve the cached entry")
	}
	if n := fs.evictInodes(time.Now()); n != 0 {
		t.Fatalf("recently listed entry was evicted: %d", n)
	}
}

func TestInodeLookupReferencesConcurrentPublication(t *testing.T) {
	fs := newTestFS()
	fs.flags.InodeCacheTTL = time.Minute
	root := fs.inodes[fuseops.RootInodeID]
	started, resume := make(chan struct{}), make(chan struct{})
	root.dir.cloud = &evictionBackend{head: func(in *HeadBlobInput) (*HeadBlobOutput, error) {
		close(started)
		<-resume
		return &HeadBlobOutput{BlobItemOutput: BlobItemOutput{Key: &in.Key}}, nil
	}}
	op := &fuseops.LookUpInodeOp{Parent: root.ID, Name: "file"}
	done := make(chan error, 1)
	go func() { done <- fs.LookUpInode(context.Background(), op) }()
	<-started
	root.mu.Lock()
	fs.mu.Lock()
	cached := idleTestInode(fs, root, "file", false, 0)
	fs.mu.Unlock()
	root.mu.Unlock()
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if op.Entry.Child != cached.ID || cached.refcnt != 1 {
		t.Fatalf("lookup returned %d with %d references, want %d and 1", op.Entry.Child, cached.refcnt, cached.ID)
	}
	if n := fs.evictInodes(time.Now()); n != 0 {
		t.Fatal("successful lookup's inode was evicted")
	}
}

func TestInodeEvictionWorkerShutdown(t *testing.T) {
	for _, stop := range []string{"destroy", "context"} {
		t.Run(stop, func(t *testing.T) {
			fs := newTestFS()
			fs.flags.InodeCacheTTL = time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fs.startInodeEviction(ctx)
			if stop == "destroy" {
				fs.Destroy()
				fs.Destroy()
			} else {
				cancel()
			}
			select {
			case <-fs.evictionDone:
			case <-time.After(time.Second):
				t.Fatal("eviction worker did not stop")
			}
		})
	}
	fs := newTestFS()
	fs.startInodeEviction(context.Background())
	fs.Destroy()
	if fs.evictionDone != nil {
		t.Fatal("disabled eviction started a worker")
	}
}

func TestInodeEvictionConcurrentLookupAndForget(t *testing.T) {
	fs := newTestFS()
	fs.flags.InodeCacheTTL = time.Minute
	root := fs.inodes[fuseops.RootInodeID]
	root.dir.cloud = &evictionBackend{head: func(in *HeadBlobInput) (*HeadBlobOutput, error) {
		return &HeadBlobOutput{BlobItemOutput: BlobItemOutput{Key: &in.Key}}, nil
	}}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 1000; i++ {
			root.mu.Lock()
			fs.mu.Lock()
			idleTestInode(fs, root, "file", false, 0)
			fs.mu.Unlock()
			root.mu.Unlock()
			op := &fuseops.LookUpInodeOp{Parent: root.ID, Name: "file"}
			if err := fs.LookUpInode(context.Background(), op); err != nil {
				done <- err
				return
			}
			fs.mu.RLock()
			inode := fs.inodes[op.Entry.Child]
			valid := inode != nil && inode.refcnt == 1
			fs.mu.RUnlock()
			if !valid {
				done <- fmt.Errorf("successful lookup lost its kernel reference")
				return
			}
			if err := fs.ForgetInode(context.Background(), &fuseops.ForgetInodeOp{Inode: op.Entry.Child, N: 1}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 1000; i++ {
		fs.evictInodes(time.Now())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	fs.evictInodes(time.Now())
	if len(fs.inodes) != 1 || len(root.dir.Children) != 2 {
		t.Fatal("concurrent eviction left stale entries")
	}
}

func TestInodeEvictionWorkerReclaimsWithoutAccess(t *testing.T) {
	fs := newTestFS()
	fs.flags.InodeCacheTTL = 100 * time.Millisecond
	root := fs.inodes[fuseops.RootInodeID]
	inode := idleTestInode(fs, root, "externally-deleted", false, 0)
	fs.startInodeEviction(context.Background())
	defer fs.Destroy()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-poll.C:
			fs.mu.RLock()
			removed := fs.inodes[inode.ID] == nil
			fs.mu.RUnlock()
			if removed {
				return
			}
		case <-deadline.C:
			t.Fatal("idle inode was not reclaimed by the background worker")
		}
	}
}

func TestInodeEvictionFlags(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{{"", 5 * time.Minute}, {"0", 0}, {"2m", 2 * time.Minute}} {
		t.Run(tc.value, func(t *testing.T) {
			app := NewApp()
			called := false
			app.Action = func(c *cli.Context) error {
				called = true
				flags := PopulateFlags(c)
				if flags.InodeCacheTTL != tc.want {
					t.Fatalf("inode cache TTL = %v, want %v", flags.InodeCacheTTL, tc.want)
				}
				return nil
			}
			args := []string{"goofys"}
			if tc.value != "" {
				args = append(args, "--inode-cache-ttl", tc.value)
			}
			args = append(args, "bucket", t.TempDir())
			if err := app.Run(args); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("CLI action was not invoked")
			}
		})
	}
}
