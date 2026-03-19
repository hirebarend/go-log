package golog

import "hash/crc32"

type Entry struct {
	Header EntryHeader
	Data   []byte
}

func NewEntry(data []byte, index uint64) *Entry {
	return &Entry{
		Header: EntryHeader{
			Length:   uint64(len(data)),
			Checksum: crc32.ChecksumIEEE(data),
			Index:    index,
		},
		Data: data,
	}
}

func (e *Entry) ToBytes() []byte {
	b := make([]byte, EntryHeaderSize+len(e.Data))
	e.Header.PutBytes(b[:EntryHeaderSize])
	copy(b[EntryHeaderSize:], e.Data)
	return b
}
