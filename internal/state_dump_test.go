package internal

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jacobsa/fuse/fuseops"
)

func TestDumpStateIncludesReferencesAndHandles(t *testing.T) {
	fs := newTestFS()
	fs.flags.InodeCacheTTL = 30 * time.Minute
	fs.bucket = "test-bucket"
	root := fs.inodes[fuseops.RootInodeID]
	dir := insertTestInode(fs, root, "dir", true, 0)
	file := insertTestInode(fs, dir, "file", false, 2)
	file.lastUsed = time.Now().Add(-time.Hour)
	file.fileHandles = 1
	fs.fileHandles = map[fuseops.HandleID]*FileHandle{7: {inode: file}}
	fs.dirHandles = map[fuseops.HandleID]*DirHandle{8: {inode: dir}}
	dir.mu.Lock()
	defer dir.mu.Unlock()
	var buffer bytes.Buffer
	if err := fs.DumpState(&buffer); err != nil {
		t.Fatal(err)
	}
	var state filesystemState
	if err := json.Unmarshal(buffer.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if state.Bucket != fs.bucket || state.InodeTTL != "30m0s" || state.InodeCount != len(fs.inodes) {
		t.Fatalf("incorrect state summary: %+v", state)
	}
	if state.FileHandles[7] != file.ID || state.DirHandles[8] != dir.ID {
		t.Fatal("missing open handles")
	}
	entries := make(map[fuseops.InodeID]inodeState)
	for i, inode := range state.Inodes {
		entries[inode.ID] = inode
		if i > 0 && inode.ID <= state.Inodes[i-1].ID {
			t.Fatal("inodes are not sorted by ID")
		}
	}
	if !entries[dir.ID].Busy {
		t.Fatal("locked directory was not marked busy")
	}
	entry := entries[file.ID]
	if entry.Name != "file" || entry.Parent != dir.ID || entry.References != 2 || entry.FileHandles != 1 ||
		!entry.LastUsed.Equal(file.lastUsed) {
		t.Fatalf("incorrect inode state: %+v", entry)
	}
}

type stateDumpWriter struct {
	fs *Goofys
}

func (w stateDumpWriter) Write([]byte) (int, error) {
	if !w.fs.mu.TryLock() {
		return 0, errors.New("filesystem locked during state dump write")
	}
	w.fs.mu.Unlock()
	return 0, io.ErrClosedPipe
}

func TestDumpStateReleasesLocksAndReturnsWriteErrors(t *testing.T) {
	fs := newTestFS()
	if err := fs.DumpState(stateDumpWriter{fs}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("DumpState() error = %v; want ErrClosedPipe", err)
	}
}
