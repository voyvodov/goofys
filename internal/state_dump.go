package internal

import (
	"encoding/json"
	"io"
	"runtime"
	"time"

	"github.com/jacobsa/fuse/fuseops"
)

type inodeState struct {
	ID          fuseops.InodeID   `json:"id"`
	Parent      fuseops.InodeID   `json:"parent,omitempty"`
	Name        string            `json:"name"`
	References  uint64            `json:"references"`
	LastUsed    time.Time         `json:"last_used"`
	AttrTime    time.Time         `json:"attribute_time"`
	Size        uint64            `json:"size"`
	FileHandles int32             `json:"file_handles"`
	Directory   bool              `json:"directory"`
	Children    []fuseops.InodeID `json:"children,omitempty"`
	ImplicitDir bool              `json:"implicit_directory"`
	Invalid     bool              `json:"invalid"`
	Busy        bool              `json:"busy,omitempty"`
	Removed     bool              `json:"removed_during_dump,omitempty"`
}

type filesystemState struct {
	CapturedAt   time.Time `json:"captured_at"`
	Bucket       string    `json:"bucket"`
	InodeTTL     string    `json:"inode_cache_ttl"`
	Forgotten    uint32    `json:"forgotten_inodes"`
	ActiveSlurps int       `json:"active_slurps"`
	InodeCount   int       `json:"inode_count"`
	// Inodes       []inodeState                         `json:"inodes"`
	FileHandles map[fuseops.HandleID]fuseops.InodeID `json:"file_handles"`
	DirHandles  map[fuseops.HandleID]fuseops.InodeID `json:"directory_handles"`
	Goroutines  int                                  `json:"goroutines"`
	HeapBytes   uint64                               `json:"heap_allocated_bytes"`
	HeapObjects uint64                               `json:"heap_objects"`
}

// DumpState writes a best-effort diagnostic snapshot without querying storage.
// Inodes are inspected individually; busy entries are marked rather than waited
// for. No filesystem locks are held while writing to w.
func (fs *Goofys) DumpState(w io.Writer) error {
	state := filesystemState{
		CapturedAt:  time.Now(),
		Bucket:      fs.bucket,
		FileHandles: make(map[fuseops.HandleID]fuseops.InodeID),
		DirHandles:  make(map[fuseops.HandleID]fuseops.InodeID),
	}
	if fs.flags != nil {
		state.InodeTTL = fs.flags.InodeCacheTTL.String()
	}
	fs.mu.RLock()
	state.Forgotten = fs.forgotCnt
	state.ActiveSlurps = fs.activeSlurps
	state.InodeCount = len(fs.inodes)
	// inodes := make([]*Inode, 0, len(fs.inodes))
	// for _, inode := range fs.inodes {
	// 	inodes = append(inodes, inode)
	// }
	for id, handle := range fs.fileHandles {
		state.FileHandles[id] = handle.inode.ID
	}
	for id, handle := range fs.dirHandles {
		state.DirHandles[id] = handle.inode.ID
	}
	fs.mu.RUnlock()
	// sort.Slice(inodes, func(i, j int) bool { return inodes[i].ID < inodes[j].ID })
	// state.Inodes = make([]inodeState, 0, len(inodes))
	// for _, inode := range inodes {
	// 	locked := inode.mu.TryLock()
	// 	fs.mu.RLock()
	// 	entry := inodeState{
	// 		ID:          inode.ID,
	// 		References:  inode.refcnt,
	// 		LastUsed:    inode.lastUsed,
	// 		FileHandles: atomic.LoadInt32(&inode.fileHandles),
	// 		Busy:        !locked,
	// 		Removed:     fs.inodes[inode.ID] != inode,
	// 	}
	// 	if inode.Parent != nil {
	// 		entry.Parent = inode.Parent.ID
	// 	}
	// 	if locked && !entry.Removed {
	// 		entry.Name = *inode.Name
	// 		entry.AttrTime = inode.AttrTime
	// 		entry.Size = inode.Attributes.Size
	// 		entry.Directory = inode.isDir()
	// 		entry.ImplicitDir = inode.ImplicitDir
	// 		entry.Invalid = inode.Invalid
	// 		if inode.isDir() {
	// 			for _, child := range inode.dir.Children {
	// 				entry.Children = append(entry.Children, child.ID)
	// 			}
	// 		}
	// 	}
	// 	fs.mu.RUnlock()
	// 	if locked {
	// 		inode.mu.Unlock()
	// 	}
	// 	state.Inodes = append(state.Inodes, entry)
	// }
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	state.Goroutines = runtime.NumGoroutine()
	state.HeapBytes = memory.HeapAlloc
	state.HeapObjects = memory.HeapObjects
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(state)
}
