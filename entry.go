package golog

import (
	"bytes"
	"hash/crc32"
)

type Entry struct {
	Header EntryHeader
	Data   []byte
}

func NewEntry(data []byte, index uint64) (*Entry, error) {
	length := len(data)

	checksum := crc32.ChecksumIEEE(data)

	return &Entry{
		Header: EntryHeader{
			Length:   uint64(length),
			Checksum: checksum,
			Index:    index,
		},
		Data: data,
	}, nil
}

func (e *Entry) ToBytes() ([]byte, error) {
	var buffer bytes.Buffer

	if _, err := buffer.Write(e.Header.ToBytes()); err != nil {
		return nil, err
	}

	if _, err := buffer.Write(e.Data); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
