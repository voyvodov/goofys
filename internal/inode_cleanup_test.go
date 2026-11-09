package internal

import (
	"testing"

	. "github.com/voyvodov/goofys/api/common"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/jacobsa/fuse/fuseops"
	. "gopkg.in/check.v1"
)

type InodeCleanupTest struct{}

var _ = Suite(&InodeCleanupTest{})

func TestCollectUnreferencedDescendantsConcurrentReferences(t *testing.T) {
	fs := newTestFS()
	root := fs.inodes[fuseops.RootInodeID]
	dir := insertTestInode(fs, root, "dir", true, 0)
	child := insertTestInode(fs, dir, "child", false, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			dir.mu.Lock()
			fs.mu.Lock()
			child.Ref()
			child.DeRef(1)
			fs.mu.Unlock()
			dir.mu.Unlock()
		}
	}()
	for i := 0; i < 10000; i++ {
		var candidates []*Inode
		fs.collectUnreferencedDescendants(dir, &candidates)
		if len(candidates) != 1 || candidates[0] != dir {
			t.Error("cleanup selected a referenced child")
			break
		}
	}
	<-done
}

// newTestFS creates a minimal Goofys with a root inode for unit tests
// that don't need a storage backend.
func newTestFS() *Goofys {
	fs := &Goofys{
		flags: &FlagStorage{},
	}
	fs.nextInodeID = fuseops.RootInodeID + 1
	fs.inodes = make(map[fuseops.InodeID]*Inode)

	root := NewInode(fs, nil, aws.String(""))
	root.ID = fuseops.RootInodeID
	root.ToDir()
	fs.inodes[fuseops.RootInodeID] = root
	fs.addDotAndDotDot(root)
	return fs
}

// insertTestInode creates an inode under parent, inserts it into
// fs.inodes, and sets its refcnt.
func insertTestInode(fs *Goofys, parent *Inode, name string, dir bool, refcnt uint64) *Inode {
	inode := NewInode(fs, parent, aws.String(name))
	if dir {
		inode.ToDir()
	}
	inode.refcnt = 0
	fs.insertInode(parent, inode)
	inode.refcnt = refcnt
	return inode
}

func (s *InodeCleanupTest) TestForgetRetainsReferencedChildren(t *C) {
	// Build: root -> dirA (refcnt=1) -> dirB (refcnt=1) -> fileC (refcnt=0)
	// Forget dirA. dirB has a kernel reference so it must survive.
	// fileC is under dirB and must also survive — it's dirB's
	// responsibility to clean up when dirB is forgotten.
	fs := newTestFS()
	root := fs.inodes[fuseops.RootInodeID]

	dirA := insertTestInode(fs, root, "dirA", true, 1)
	dirB := insertTestInode(fs, dirA, "dirB", true, 1)
	fileC := insertTestInode(fs, dirB, "fileC", false, 0)

	dirAID := dirA.ID
	dirBID := dirB.ID
	fileCID := fileC.ID

	// Verify all inodes exist
	t.Assert(fs.inodes[dirAID], NotNil)
	t.Assert(fs.inodes[dirBID], NotNil)
	t.Assert(fs.inodes[fileCID], NotNil)

	// Forget dirA (simulates kernel ForgetInode)
	err := fs.ForgetInode(nil, &fuseops.ForgetInodeOp{Inode: dirAID, N: 1})
	t.Assert(err, IsNil)

	// dirA should be gone
	t.Assert(fs.inodes[dirAID], IsNil)

	// dirB must survive — it has refcnt > 0
	t.Assert(fs.inodes[dirBID], NotNil)

	// fileC must also survive — it's a child of the retained dirB
	t.Assert(fs.inodes[fileCID], NotNil)
}

func (s *InodeCleanupTest) TestForgetCleansUnreferencedSubtree(t *C) {
	// Build: root -> dirA (refcnt=1) -> dirB (refcnt=0) -> fileC (refcnt=0)
	// When dirA is forgotten, the entire subtree should be cleaned.
	fs := newTestFS()
	root := fs.inodes[fuseops.RootInodeID]

	dirA := insertTestInode(fs, root, "dirA", true, 1)
	dirB := insertTestInode(fs, dirA, "dirB", true, 0)
	fileC := insertTestInode(fs, dirB, "fileC", false, 0)

	dirAID := dirA.ID
	dirBID := dirB.ID
	fileCID := fileC.ID

	err := fs.ForgetInode(nil, &fuseops.ForgetInodeOp{Inode: dirAID, N: 1})
	t.Assert(err, IsNil)

	// All three should be gone
	t.Assert(fs.inodes[dirAID], IsNil)
	t.Assert(fs.inodes[dirBID], IsNil)
	t.Assert(fs.inodes[fileCID], IsNil)
}

func (s *InodeCleanupTest) TestForgetRetainsMixedRefcntTree(t *C) {
	// Build: root -> dirA (refcnt=1) -> dirB (refcnt=0)
	//                                -> dirC (refcnt=1) -> fileD (refcnt=0)
	// Forget dirA. dirB should be cleaned. dirC should survive,
	// and fileD should survive because it's under retained dirC.
	fs := newTestFS()
	root := fs.inodes[fuseops.RootInodeID]

	dirA := insertTestInode(fs, root, "dirA", true, 1)
	dirB := insertTestInode(fs, dirA, "dirB", true, 0)
	dirC := insertTestInode(fs, dirA, "dirC", true, 1)
	fileD := insertTestInode(fs, dirC, "fileD", false, 0)

	dirAID := dirA.ID
	dirBID := dirB.ID
	dirCID := dirC.ID
	fileDID := fileD.ID

	err := fs.ForgetInode(nil, &fuseops.ForgetInodeOp{Inode: dirAID, N: 1})
	t.Assert(err, IsNil)

	t.Assert(fs.inodes[dirAID], IsNil)
	t.Assert(fs.inodes[dirBID], IsNil)
	t.Assert(fs.inodes[dirCID], NotNil)
	t.Assert(fs.inodes[fileDID], NotNil)
}

func (s *InodeCleanupTest) TestRetainedChildStillLookupable(t *C) {
	// After forgetting a parent, a retained child's inode ID must
	// still resolve in fs.inodes — getInodeOrDie must not panic.
	fs := newTestFS()
	root := fs.inodes[fuseops.RootInodeID]

	dirA := insertTestInode(fs, root, "dirA", true, 1)
	dirB := insertTestInode(fs, dirA, "dirB", true, 1)
	insertTestInode(fs, dirB, "fileC", false, 0)

	dirBID := dirB.ID

	err := fs.ForgetInode(nil, &fuseops.ForgetInodeOp{Inode: dirA.ID, N: 1})
	t.Assert(err, IsNil)

	// dirB is retained — getInodeOrDie must not panic
	fs.mu.RLock()
	recovered := fs.inodes[dirBID]
	fs.mu.RUnlock()
	t.Assert(recovered, NotNil)
	t.Assert(recovered.ID, Equals, dirBID)

	// dirB's children should also be in fs.inodes
	dirB.mu.Lock()
	for _, child := range dirB.dir.Children {
		if *child.Name == "." || *child.Name == ".." {
			continue
		}
		fs.mu.RLock()
		t.Assert(fs.inodes[child.ID], NotNil)
		fs.mu.RUnlock()
	}
	dirB.mu.Unlock()
}

func (s *InodeCleanupTest) TestUnlinkCleansUnreferencedInode(t *C) {
	// Unlink of a refcnt=0 file should remove it from fs.inodes.
	fs := newTestFS()
	root := fs.inodes[fuseops.RootInodeID]

	file := insertTestInode(fs, root, "file1", false, 0)
	fileID := file.ID

	t.Assert(fs.inodes[fileID], NotNil)

	root.mu.Lock()
	root.removeChildUnlocked(file)
	file.Parent = nil
	root.mu.Unlock()

	fs.mu.RLock()
	unreferenced := file.refcnt == 0
	fs.mu.RUnlock()
	if unreferenced {
		fs.removeInodeFromMap(file)
	}

	t.Assert(fs.inodes[fileID], IsNil)
}

func (s *InodeCleanupTest) TestUnlinkRetainsReferencedInode(t *C) {
	// Unlink of a refcnt > 0 file should keep it in fs.inodes.
	fs := newTestFS()
	root := fs.inodes[fuseops.RootInodeID]

	file := insertTestInode(fs, root, "file1", false, 1)
	fileID := file.ID

	root.mu.Lock()
	root.removeChildUnlocked(file)
	file.Parent = nil
	root.mu.Unlock()

	fs.mu.RLock()
	unreferenced := file.refcnt == 0
	fs.mu.RUnlock()
	if unreferenced {
		fs.removeInodeFromMap(file)
	}

	t.Assert(fs.inodes[fileID], NotNil)
}
