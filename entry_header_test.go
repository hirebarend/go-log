package golog_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	golog "github.com/hirebarend/go-log"
)

func TestEntryHeader_EntryHeaderSize(t *testing.T) {
	entryHeader := golog.EntryHeader{
		Checksum: 0x75,
		Index:    0x123,
		Length:   0x456,
	}

	data := entryHeader.ToBytes()

	if len(data) != golog.EntryHeaderSize {
		t.Fatalf("EntryHeaderSize = %v, want %v", golog.EntryHeaderSize, len(data))
	}
}

func TestEntryHeader_NewEntryHeaderFromBytes(t *testing.T) {
	data := make([]byte, golog.EntryHeaderSize)
	binary.LittleEndian.PutUint64(data[0:8], 0x456)
	binary.LittleEndian.PutUint32(data[8:12], 0x75)
	binary.LittleEndian.PutUint64(data[12:20], 0x123)

	entryHeader := golog.NewEntryHeaderFromBytes(data)

	if entryHeader.Checksum != 0x75 {
		t.Fatalf("NewEntryHeaderFromBytes(data) entryHeader.Checksum = %v, want %v", entryHeader.Checksum, 0x75)
	}

	if entryHeader.Index != 0x123 {
		t.Fatalf("NewEntryHeaderFromBytes(data) entryHeader.Index = %v, want %v", entryHeader.Checksum, 0x75)
	}

	if entryHeader.Length != 0x456 {
		t.Fatalf("NewEntryHeaderFromBytes(data) entryHeader.Length = %v, want %v", entryHeader.Checksum, 0x75)
	}
}

func TestEntryHeader_ToBytes(t *testing.T) {
	entryHeader := golog.EntryHeader{
		Checksum: 0x75,
		Index:    0x123,
		Length:   0x456,
	}

	got := entryHeader.ToBytes()

	want := make([]byte, golog.EntryHeaderSize)
	binary.LittleEndian.PutUint64(want[0:8], entryHeader.Length)
	binary.LittleEndian.PutUint32(want[8:12], entryHeader.Checksum)
	binary.LittleEndian.PutUint64(want[12:20], entryHeader.Index)

	if !bytes.Equal(got, want) {
		t.Fatalf("ToBytes() = %v, want %v", got, want)
	}
}
