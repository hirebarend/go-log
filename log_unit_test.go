package golog_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	golog "github.com/hirebarend/go-log"
)

// helper creates a temporary directory and a log with the given max segment size.
// It returns the log and a cleanup function that removes the temporary directory.
func newTestLog(t *testing.T, maxSegmentSize uint64) (*golog.Log, string) {
	t.Helper()
	dir := t.TempDir()
	log := golog.NewLog(dir, maxSegmentSize)
	return log, dir
}

// ---------------------------------------------------------------------------
// Basic Operations
// ---------------------------------------------------------------------------

func TestWrite_SingleEntry(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	idx, err := log.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if idx != 1 {
		t.Fatalf("expected index 1, got %d", idx)
	}
}

func TestWrite_MultipleEntries(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 1; i <= 100; i++ {
		idx, err := log.Write([]byte(fmt.Sprintf("entry-%d", i)))
		if err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		if idx != uint64(i) {
			t.Fatalf("expected index %d, got %d", i, idx)
		}
	}
}

func TestWrite_ThenRead(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	want := []byte("raft-entry")
	idx, err := log.Write(want)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, err := log.Read(idx)
	if err != nil {
		t.Fatalf("Read(%d): %v", idx, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Read(%d) = %q, want %q", idx, got, want)
	}
}

func TestRead_MultipleEntries(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	entries := make([][]byte, 50)
	for i := range entries {
		entries[i] = []byte(fmt.Sprintf("data-%d", i))
		if _, err := log.Write(entries[i]); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	for i, want := range entries {
		idx := uint64(i + 1)
		got, err := log.Read(idx)
		if err != nil {
			t.Fatalf("Read(%d): %v", idx, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read(%d) = %q, want %q", idx, got, want)
		}
	}
}

func TestRead_EmptyLog_ReturnsError(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	_, err := log.Read(1)
	if err == nil {
		t.Fatal("expected error reading from empty log")
	}
}

func TestRead_InvalidIndex_ReturnsError(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	if _, err := log.Write([]byte("only-entry")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Index 0 is before the first entry
	if _, err := log.Read(0); err == nil {
		t.Fatal("expected error reading index 0")
	}

	// Index 2 is past the last entry
	if _, err := log.Read(2); err == nil {
		t.Fatal("expected error reading index 2")
	}
}

func TestWrite_EmptyData(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	idx, err := log.Write([]byte{})
	if err != nil {
		t.Fatalf("Write empty: %v", err)
	}
	if idx != 1 {
		t.Fatalf("expected index 1, got %d", idx)
	}

	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, err := log.Read(1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty data, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Index Management
// ---------------------------------------------------------------------------

func TestGetLastIndex_EmptyLog(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	idx, err := log.GetLastIndex()
	if err != nil {
		t.Fatalf("GetLastIndex: %v", err)
	}
	if idx != 0 {
		t.Fatalf("expected 0, got %d", idx)
	}
}

func TestGetLastIndex_AfterWrites(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 10; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	idx, err := log.GetLastIndex()
	if err != nil {
		t.Fatalf("GetLastIndex: %v", err)
	}
	if idx != 10 {
		t.Fatalf("expected 10, got %d", idx)
	}
}

func TestGetCommittedIndex_EmptyLog(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	idx, err := log.GetCommittedIndex()
	if err != nil {
		t.Fatalf("GetCommittedIndex: %v", err)
	}
	if idx != 0 {
		t.Fatalf("expected 0, got %d", idx)
	}
}

func TestGetCommittedIndex_BeforeCommit(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 5; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// CommittedIndex should be 0 before any explicit Commit
	idx, err := log.GetCommittedIndex()
	if err != nil {
		t.Fatalf("GetCommittedIndex: %v", err)
	}
	if idx != 0 {
		t.Fatalf("expected 0 before commit, got %d", idx)
	}
}

func TestGetCommittedIndex_AfterCommit(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 5; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	idx, err := log.GetCommittedIndex()
	if err != nil {
		t.Fatalf("GetCommittedIndex: %v", err)
	}
	if idx != 5 {
		t.Fatalf("expected 5 after commit, got %d", idx)
	}
}

func TestGetCommittedIndex_IncrementalCommits(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	// Write 3, commit
	for i := 0; i < 3; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	idx, _ := log.GetCommittedIndex()
	if idx != 3 {
		t.Fatalf("expected committed index 3, got %d", idx)
	}

	// Write 2 more, commit
	for i := 0; i < 2; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	idx, _ = log.GetCommittedIndex()
	if idx != 5 {
		t.Fatalf("expected committed index 5, got %d", idx)
	}
}

// ---------------------------------------------------------------------------
// Commit / Durability
// ---------------------------------------------------------------------------

func TestCommit_EmptyLog(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	// Commit on an empty log should not error
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit on empty log: %v", err)
	}
}

func TestWriteCommit(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	idx, err := log.WriteCommit([]byte("committed-entry"))
	if err != nil {
		t.Fatalf("WriteCommit: %v", err)
	}
	if idx != 1 {
		t.Fatalf("expected index 1, got %d", idx)
	}

	// After WriteCommit, the committed index should be updated
	committedIdx, err := log.GetCommittedIndex()
	if err != nil {
		t.Fatalf("GetCommittedIndex: %v", err)
	}
	if committedIdx < 1 {
		t.Fatalf("expected committed index >= 1 after WriteCommit, got %d", committedIdx)
	}

	// Data should be readable
	got, err := log.Read(1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, []byte("committed-entry")) {
		t.Fatalf("Read = %q, want %q", got, "committed-entry")
	}
}

// ---------------------------------------------------------------------------
// Persistence / Recovery (Load)
// ---------------------------------------------------------------------------

func TestLoad_RecoverAfterCommitAndClose(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write, commit, close
	log1 := golog.NewLog(dir, 1<<20)
	for i := 0; i < 20; i++ {
		if _, err := log1.Write([]byte(fmt.Sprintf("entry-%d", i+1))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log1.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := log1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: reopen and verify
	log2 := golog.NewLog(dir, 1<<20)
	if err := log2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer log2.Close()

	lastIdx, _ := log2.GetLastIndex()
	if lastIdx != 20 {
		t.Fatalf("expected last index 20 after reload, got %d", lastIdx)
	}

	for i := 1; i <= 20; i++ {
		want := []byte(fmt.Sprintf("entry-%d", i))
		got, err := log2.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d) after reload: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestLoad_CommittedIndexPreserved(t *testing.T) {
	dir := t.TempDir()

	log1 := golog.NewLog(dir, 1<<20)
	for i := 0; i < 10; i++ {
		if _, err := log1.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log1.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := log1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	log2 := golog.NewLog(dir, 1<<20)
	if err := log2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer log2.Close()

	committedIdx, _ := log2.GetCommittedIndex()
	if committedIdx != 10 {
		t.Fatalf("expected committed index 10 after reload, got %d", committedIdx)
	}
}

func TestLoad_EmptyDir(t *testing.T) {
	dir := t.TempDir()

	log := golog.NewLog(dir, 1<<20)
	if err := log.Load(); err != nil {
		t.Fatalf("Load on empty dir: %v", err)
	}
	defer log.Close()

	idx, _ := log.GetLastIndex()
	if idx != 0 {
		t.Fatalf("expected 0 after loading empty dir, got %d", idx)
	}
}

func TestLoad_IgnoresNonSegmentFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a non-.seg file in the directory
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignore me"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	log := golog.NewLog(dir, 1<<20)
	if err := log.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer log.Close()

	idx, _ := log.GetLastIndex()
	if idx != 0 {
		t.Fatalf("expected 0, got %d", idx)
	}
}

func TestLoad_ContinueWritingAfterReload(t *testing.T) {
	dir := t.TempDir()

	// Write 5 entries, commit, close
	log1 := golog.NewLog(dir, 1<<20)
	for i := 0; i < 5; i++ {
		if _, err := log1.Write([]byte("batch1")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log1.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := log1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reload and write 5 more
	log2 := golog.NewLog(dir, 1<<20)
	if err := log2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	for i := 0; i < 5; i++ {
		idx, err := log2.Write([]byte("batch2"))
		if err != nil {
			t.Fatalf("Write after reload: %v", err)
		}
		expectedIdx := uint64(6 + i)
		if idx != expectedIdx {
			t.Fatalf("expected index %d, got %d", expectedIdx, idx)
		}
	}

	if err := log2.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	lastIdx, _ := log2.GetLastIndex()
	if lastIdx != 10 {
		t.Fatalf("expected last index 10, got %d", lastIdx)
	}

	log2.Close()
}

// ---------------------------------------------------------------------------
// TruncateFrom (Raft Rollback)
// ---------------------------------------------------------------------------

func TestTruncateFrom_RemovesEntriesFromIndex(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 10; i++ {
		if _, err := log.Write([]byte(fmt.Sprintf("entry-%d", i+1))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Truncate from index 6 onward (remove entries 6-10)
	if err := log.TruncateFrom(6); err != nil {
		t.Fatalf("TruncateFrom(6): %v", err)
	}

	// Entries 1-5 should still be readable
	for i := 1; i <= 5; i++ {
		want := []byte(fmt.Sprintf("entry-%d", i))
		got, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d) after truncate: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read(%d) = %q, want %q", i, got, want)
		}
	}

	// Entries 6-10 should error
	for i := 6; i <= 10; i++ {
		_, err := log.Read(uint64(i))
		if err == nil {
			t.Fatalf("expected error reading truncated index %d", i)
		}
	}
}

func TestTruncateFrom_UpdatesLastIndex(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 10; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if err := log.TruncateFrom(6); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}

	idx, _ := log.GetLastIndex()
	if idx != 5 {
		t.Fatalf("expected last index 5 after TruncateFrom(6), got %d", idx)
	}
}

func TestTruncateFrom_EmptyLog(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	if err := log.TruncateFrom(1); err != nil {
		t.Fatalf("TruncateFrom on empty log: %v", err)
	}
}

func TestTruncateFrom_AllEntries(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 5; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Truncate from 1 removes everything
	if err := log.TruncateFrom(1); err != nil {
		t.Fatalf("TruncateFrom(1): %v", err)
	}

	idx, _ := log.GetLastIndex()
	if idx != 0 {
		t.Fatalf("expected last index 0, got %d", idx)
	}
}

func TestTruncateFrom_ThenWriteNewEntries(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	// Write entries 1-10
	for i := 0; i < 10; i++ {
		if _, err := log.Write([]byte("old")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Raft rollback: truncate from 6
	if err := log.TruncateFrom(6); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}

	// Write new entries starting at 6
	for i := 0; i < 3; i++ {
		idx, err := log.Write([]byte("new"))
		if err != nil {
			t.Fatalf("Write after truncate: %v", err)
		}
		expectedIdx := uint64(6 + i)
		if idx != expectedIdx {
			t.Fatalf("expected index %d, got %d", expectedIdx, idx)
		}
	}

	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify old entries are intact
	for i := 1; i <= 5; i++ {
		got, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d): %v", i, err)
		}
		if !bytes.Equal(got, []byte("old")) {
			t.Fatalf("Read(%d) = %q, want %q", i, got, "old")
		}
	}

	// Verify new entries
	for i := 6; i <= 8; i++ {
		got, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d): %v", i, err)
		}
		if !bytes.Equal(got, []byte("new")) {
			t.Fatalf("Read(%d) = %q, want %q", i, got, "new")
		}
	}

	idx, _ := log.GetLastIndex()
	if idx != 8 {
		t.Fatalf("expected last index 8, got %d", idx)
	}
}

func TestTruncateFrom_BeyondLastIndex(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 5; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Truncate beyond the last index should be a no-op
	if err := log.TruncateFrom(100); err != nil {
		t.Fatalf("TruncateFrom(100): %v", err)
	}

	idx, _ := log.GetLastIndex()
	if idx != 5 {
		t.Fatalf("expected last index 5, got %d", idx)
	}
}

// ---------------------------------------------------------------------------
// TruncateTo (Log Compaction)
// ---------------------------------------------------------------------------

func TestTruncateTo_RemovesEntriesUpToIndex(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 10; i++ {
		if _, err := log.Write([]byte(fmt.Sprintf("entry-%d", i+1))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Remove entries up to and including index 5
	if err := log.TruncateTo(5); err != nil {
		t.Fatalf("TruncateTo(5): %v", err)
	}

	// Entries 1-5 should no longer be readable
	for i := 1; i <= 5; i++ {
		_, err := log.Read(uint64(i))
		if err == nil {
			t.Fatalf("expected error reading compacted index %d", i)
		}
	}

	// Entries 6-10 should still be readable
	for i := 6; i <= 10; i++ {
		want := []byte(fmt.Sprintf("entry-%d", i))
		got, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d) after compaction: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestTruncateTo_EmptyLog(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	if err := log.TruncateTo(5); err != nil {
		t.Fatalf("TruncateTo on empty log: %v", err)
	}
}

func TestTruncateTo_AllEntries(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 5; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Compact everything
	if err := log.TruncateTo(5); err != nil {
		t.Fatalf("TruncateTo(5): %v", err)
	}

	idx, _ := log.GetLastIndex()
	if idx != 0 {
		t.Fatalf("expected last index 0 after full compaction, got %d", idx)
	}
}

// ---------------------------------------------------------------------------
// Automatic Segmentation
// ---------------------------------------------------------------------------

func TestSegmentation_NewSegmentOnOverflow(t *testing.T) {
	// Use a very small segment size to force segment rotation quickly.
	// Entry header is 20 bytes + data. With maxSegmentSize=100, a few entries
	// should trigger a new segment.
	log, _ := newTestLog(t, 100)
	defer log.Close()

	var lastIdx uint64
	for i := 0; i < 20; i++ {
		idx, err := log.Write([]byte(fmt.Sprintf("data-%02d", i)))
		if err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		lastIdx = idx
	}

	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if lastIdx != 20 {
		t.Fatalf("expected last index 20, got %d", lastIdx)
	}

	// The log should have created multiple segments
	if len(log.Segments) <= 1 {
		t.Fatalf("expected multiple segments, got %d", len(log.Segments))
	}
}

func TestSegmentation_ReadAcrossSegments(t *testing.T) {
	log, _ := newTestLog(t, 100)
	defer log.Close()

	entries := make([][]byte, 20)
	for i := range entries {
		entries[i] = []byte(fmt.Sprintf("entry-%02d", i))
		if _, err := log.Write(entries[i]); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	for i, want := range entries {
		idx := uint64(i + 1)
		got, err := log.Read(idx)
		if err != nil {
			t.Fatalf("Read(%d): %v", idx, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read(%d) = %q, want %q", idx, got, want)
		}
	}
}

func TestSegmentation_TruncateFromAcrossSegments(t *testing.T) {
	log, _ := newTestLog(t, 100)
	defer log.Close()

	for i := 0; i < 20; i++ {
		if _, err := log.Write([]byte(fmt.Sprintf("data-%02d", i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	segsBefore := len(log.Segments)

	if err := log.TruncateFrom(5); err != nil {
		t.Fatalf("TruncateFrom(5): %v", err)
	}

	segsAfter := len(log.Segments)
	if segsAfter >= segsBefore {
		t.Fatalf("expected fewer segments after truncation: before=%d, after=%d", segsBefore, segsAfter)
	}

	// Entries 1-4 should still be readable
	for i := 1; i <= 4; i++ {
		if _, err := log.Read(uint64(i)); err != nil {
			t.Fatalf("Read(%d) after cross-segment truncate: %v", i, err)
		}
	}
}

func TestSegmentation_TruncateToAcrossSegments(t *testing.T) {
	log, _ := newTestLog(t, 100)
	defer log.Close()

	for i := 0; i < 20; i++ {
		if _, err := log.Write([]byte(fmt.Sprintf("data-%02d", i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	segsBefore := len(log.Segments)

	if err := log.TruncateTo(15); err != nil {
		t.Fatalf("TruncateTo(15): %v", err)
	}

	segsAfter := len(log.Segments)
	if segsAfter >= segsBefore {
		t.Fatalf("expected fewer segments after compaction: before=%d, after=%d", segsBefore, segsAfter)
	}

	// Entries 16-20 should still be readable
	for i := 16; i <= 20; i++ {
		if _, err := log.Read(uint64(i)); err != nil {
			t.Fatalf("Read(%d) after cross-segment compaction: %v", i, err)
		}
	}
}

func TestSegmentation_ReloadMultipleSegments(t *testing.T) {
	dir := t.TempDir()

	log1 := golog.NewLog(dir, 100)
	for i := 0; i < 20; i++ {
		if _, err := log1.Write([]byte(fmt.Sprintf("data-%02d", i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log1.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	segCount := len(log1.Segments)
	if segCount <= 1 {
		t.Fatalf("expected multiple segments, got %d", segCount)
	}

	if err := log1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reload
	log2 := golog.NewLog(dir, 100)
	if err := log2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer log2.Close()

	if len(log2.Segments) != segCount {
		t.Fatalf("expected %d segments after reload, got %d", segCount, len(log2.Segments))
	}

	// All entries readable
	for i := 1; i <= 20; i++ {
		if _, err := log2.Read(uint64(i)); err != nil {
			t.Fatalf("Read(%d) after multi-segment reload: %v", i, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestClose_FlushesAndCloses(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)

	for i := 0; i < 5; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After close, segments list should be empty
	if len(log.Segments) != 0 {
		t.Fatalf("expected 0 segments after close, got %d", len(log.Segments))
	}
}

func TestClose_EmptyLog(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)

	if err := log.Close(); err != nil {
		t.Fatalf("Close on empty log: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Data Integrity (CRC32)
// ---------------------------------------------------------------------------

func TestDataIntegrity_CorruptedSegmentFile(t *testing.T) {
	dir := t.TempDir()

	log1 := golog.NewLog(dir, 1<<20)
	if _, err := log1.Write([]byte("important data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := log1.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := log1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Find and corrupt the segment file
	segFiles, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segFiles) == 0 {
		t.Fatal("no segment files found")
	}

	// Corrupt the data portion (after the 20-byte header)
	f, err := os.OpenFile(segFiles[0], os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	// Write garbage into the data area
	if _, err := f.WriteAt([]byte{0xFF, 0xFF, 0xFF}, 20); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	f.Close()

	// Reload and attempt to read — should get a checksum error
	log2 := golog.NewLog(dir, 1<<20)
	if err := log2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer log2.Close()

	_, err = log2.Read(1)
	if err == nil {
		t.Fatal("expected checksum error reading corrupted entry")
	}
}

// ---------------------------------------------------------------------------
// Concurrent Access
// ---------------------------------------------------------------------------

func TestConcurrent_Writes(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	const goroutines = 10
	const writesPerGoroutine = 50

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*writesPerGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				_, err := log.Write([]byte(fmt.Sprintf("g%d-i%d", id, i)))
				if err != nil {
					errs <- err
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent write error: %v", err)
	}

	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	lastIdx, _ := log.GetLastIndex()
	expected := uint64(goroutines * writesPerGoroutine)
	if lastIdx != expected {
		t.Fatalf("expected last index %d, got %d", expected, lastIdx)
	}
}

func TestConcurrent_ReadWhileWriting(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	// Pre-populate with some committed entries
	for i := 0; i < 50; i++ {
		if _, err := log.Write([]byte(fmt.Sprintf("pre-%d", i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// Writers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if _, err := log.Write([]byte(fmt.Sprintf("new-%d", i))); err != nil {
				errs <- fmt.Errorf("write: %w", err)
			}
		}
	}()

	// Readers (read pre-existing entries)
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1; i <= 50; i++ {
				_, err := log.Read(uint64(i))
				if err != nil {
					errs <- fmt.Errorf("read(%d): %w", i, err)
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// WriteCommit batch behavior
// ---------------------------------------------------------------------------

func TestWriteCommit_MultipleWaiters(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	const n = 10
	var wg sync.WaitGroup
	results := make(chan uint64, n)
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			idx, err := log.WriteCommit([]byte(fmt.Sprintf("waiter-%d", id)))
			if err != nil {
				errs <- err
				return
			}
			results <- idx
		}(i)
	}

	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("WriteCommit error: %v", err)
	}

	seen := make(map[uint64]bool)
	for idx := range results {
		if seen[idx] {
			t.Fatalf("duplicate index %d", idx)
		}
		seen[idx] = true
	}

	if len(seen) != n {
		t.Fatalf("expected %d unique indices, got %d", n, len(seen))
	}
}

// ---------------------------------------------------------------------------
// Edge Cases: Write after TruncateFrom all entries (re-initialization)
// ---------------------------------------------------------------------------

func TestTruncateFrom_ThenWrite_RestartsFromCorrectIndex(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 5; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Truncate everything
	if err := log.TruncateFrom(1); err != nil {
		t.Fatalf("TruncateFrom(1): %v", err)
	}

	// Write again — the log should start from index 1 again since it's empty
	idx, err := log.Write([]byte("fresh"))
	if err != nil {
		t.Fatalf("Write after full truncate: %v", err)
	}
	if idx != 1 {
		t.Fatalf("expected index 1 after full truncate+write, got %d", idx)
	}
}

// ---------------------------------------------------------------------------
// Large entry
// ---------------------------------------------------------------------------

func TestWrite_LargeEntry(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	data := make([]byte, 100_000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	idx, err := log.Write(data)
	if err != nil {
		t.Fatalf("Write large: %v", err)
	}
	if idx != 1 {
		t.Fatalf("expected index 1, got %d", idx)
	}

	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, err := log.Read(1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("large entry data mismatch")
	}
}

// ---------------------------------------------------------------------------
// Sequential index ordering
// ---------------------------------------------------------------------------

func TestIndices_AreMonotonicallyIncreasing(t *testing.T) {
	log, _ := newTestLog(t, 100) // small segments to test across boundaries
	defer log.Close()

	var prev uint64
	for i := 0; i < 30; i++ {
		idx, err := log.Write([]byte("x"))
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		if idx <= prev {
			t.Fatalf("index %d is not greater than previous %d", idx, prev)
		}
		prev = idx
	}
}

// ---------------------------------------------------------------------------
// GetLastIndex and GetCommittedIndex after TruncateFrom
// ---------------------------------------------------------------------------

func TestGetCommittedIndex_AfterTruncateFrom(t *testing.T) {
	log, _ := newTestLog(t, 1<<20)
	defer log.Close()

	for i := 0; i < 10; i++ {
		if _, err := log.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := log.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if err := log.TruncateFrom(6); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}

	committedIdx, _ := log.GetCommittedIndex()
	if committedIdx > 5 {
		t.Fatalf("expected committed index <= 5 after TruncateFrom(6), got %d", committedIdx)
	}
}
