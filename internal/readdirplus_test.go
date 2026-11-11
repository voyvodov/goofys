package internal

import (
	"context"
	"encoding/binary"
	"syscall"
	"testing"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

func TestReadDirPlusReferencesAndOffsets(t *testing.T) {
	fs := newTestFS()
	fs.flags.TypeCacheTTL = time.Hour
	fs.flags.StatCacheTTL = time.Hour
	root := fs.inodes[fuseops.RootInodeID]
	root.dir.DirTime = time.Now()
	first := insertTestInode(fs, root, "a", false, 0)
	first.Attributes.Size = 42
	second := insertTestInode(fs, root, "b", false, 0)
	fs.dirHandles = map[fuseops.HandleID]*DirHandle{1: root.OpenDir()}
	defer fs.dirHandles[1].CloseDir()
	entrySize := fuseutil.WriteDirentPlus(make([]byte, 1024), fuseutil.DirentPlus{
		Dirent: fuseutil.Dirent{Name: "a"},
	})
	read := func(offset fuseops.DirOffset, size int) *fuseops.ReadDirPlusOp {
		t.Helper()
		op := &fuseops.ReadDirPlusOp{ReadDirOp: fuseops.ReadDirOp{
			Handle: 1, Offset: offset, Dst: make([]byte, size),
		}}
		if err := fs.ReadDirPlus(context.Background(), op); err != nil {
			t.Fatal(err)
		}
		return op
	}
	if op := read(2, entrySize-1); op.BytesRead != 0 || first.refcnt != 0 || second.refcnt != 0 {
		t.Fatal("an entry that did not fit acquired a reference")
	}
	op := read(2, entrySize)
	if op.BytesRead != entrySize || first.refcnt != 1 || second.refcnt != 0 {
		t.Fatal("only the returned entry should acquire a lookup reference")
	}
	// FUSE entry_out begins with nodeid; its attributes begin at byte 40,
	// with size following the attributes' inode number.
	if binary.NativeEndian.Uint64(op.Dst[:8]) != uint64(first.ID) || binary.NativeEndian.Uint64(op.Dst[48:56]) != 42 {
		t.Fatal("READDIRPLUS did not return the child's inode and size")
	}
	if op := read(3, entrySize); op.BytesRead != entrySize || second.refcnt != 1 {
		t.Fatal("resuming a directory listing did not return the next entry")
	}
	if op := read(4, entrySize); op.BytesRead != 0 {
		t.Fatal("expected end of directory")
	}
	dotRefs := []uint64{root.dir.Children[0].refcnt, root.dir.Children[1].refcnt}
	read(0, 2*entrySize)
	if root.dir.Children[0].refcnt != dotRefs[0] || root.dir.Children[1].refcnt != dotRefs[1] {
		t.Fatal("dot entries acquired lookup references")
	}
	if err := fs.ForgetInode(context.Background(), &fuseops.ForgetInodeOp{Inode: first.ID, N: 1}); err != nil {
		t.Fatal(err)
	}
	if _, exists := fs.inodes[first.ID]; exists {
		t.Fatal("returned inode was not reclaimed after its reference was forgotten")
	}
}

func TestReadDirPlusInvalidHandle(t *testing.T) {
	if err := newTestFS().ReadDirPlus(context.Background(), &fuseops.ReadDirPlusOp{}); err != syscall.EBADF {
		t.Fatalf("ReadDirPlus() error = %v; want EBADF", err)
	}
}

func TestReadDirPlusDeletedEntryDoesNotSkipFollowingChild(t *testing.T) {
	for _, startAtDeleted := range []bool{false, true} {
		fs := newTestFS()
		fs.flags.TypeCacheTTL = time.Hour
		fs.flags.StatCacheTTL = time.Hour
		root := fs.inodes[fuseops.RootInodeID]
		root.dir.DirTime = time.Now()
		a := insertTestInode(fs, root, "a", false, 0)
		deleted := insertTestInode(fs, root, "b", false, 0)
		deleted.AttrTime = time.Now().Add(-2 * time.Hour)
		c := insertTestInode(fs, root, "c", false, 0)
		fs.dirHandles = map[fuseops.HandleID]*DirHandle{1: root.OpenDir()}
		root.dir.cloud = &evictionBackend{head: func(*HeadBlobInput) (*HeadBlobOutput, error) {
			return nil, fuse.ENOENT
		}}
		offset := fuseops.DirOffset(2)
		if startAtDeleted {
			offset = 3
		}
		op := &fuseops.ReadDirPlusOp{ReadDirOp: fuseops.ReadDirOp{
			Handle: 1, Offset: offset, Dst: make([]byte, 4096),
		}}
		if err := fs.ReadDirPlus(context.Background(), op); err != nil {
			t.Fatal(err)
		}
		if !startAtDeleted {
			if a.refcnt != 1 || c.refcnt != 0 || op.BytesRead == 0 {
				t.Fatal("partial listing lost or acquired extra references")
			}
			op.Offset = 3
			op.BytesRead = 0
			if err := fs.ReadDirPlus(context.Background(), op); err != nil {
				t.Fatal(err)
			}
		}
		if c.refcnt != 1 || op.BytesRead == 0 {
			t.Fatal("child following the deleted entry was skipped")
		}
		fs.dirHandles[1].CloseDir()
	}
}
