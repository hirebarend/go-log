package golog_test

import (
	"bytes"
	"testing"

	golog "github.com/hirebarend/go-log"
)

func TestEntry_NewEntry(t *testing.T) {
	data := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")

	entry, err := golog.NewEntry(data, 0x123)

	if err != nil {
		t.Fatalf("failed")
	}

	if !bytes.Equal(entry.Data, data) {
		t.Fatalf("entry.Data = %v, want %v", entry.Data, data)
	}

	if entry.Header.Checksum != 700066098 {
		t.Fatalf("entry.Header.Checksum = %v, want %v", entry.Header.Checksum, 700066098)
	}

	if entry.Header.Index != 291 {
		t.Fatalf("entry.Header.Index = %v, want %v", entry.Header.Index, 291)
	}

	if entry.Header.Length != uint64(len(data)) {
		t.Fatalf("entry.Header.Length = %v, want %v", entry.Header.Length, len(data))
	}
}

func TestEntry_ToBytes(t *testing.T) {
	want := []byte{56, 0, 0, 0, 0, 0, 0, 0, 50, 41, 186, 41, 35, 1, 0, 0, 0, 0, 0, 0, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 44, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 97, 100, 105, 112, 105, 115, 99, 105, 110, 103, 32, 101, 108, 105, 116, 46}

	entry, err := golog.NewEntry([]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit."), 0x123)

	if err != nil {
		t.Fatalf("failed")
	}

	got, err := entry.ToBytes()

	if err != nil {
		t.Fatalf("failed")
	}

	if !bytes.Equal(want, got) {
		t.Fatalf("ToBytes() = %v, want %v", got, want)
	}
}
