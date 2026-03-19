package golog

import "encoding/binary"

const EntryHeaderSize = 8 + 4 + 8

type EntryHeader struct {
	Length   uint64
	Checksum uint32
	Index    uint64
}

func NewEntryHeaderFromBytes(data []byte) EntryHeader {
	return EntryHeader{
		Length:   binary.LittleEndian.Uint64(data[0:8]),
		Checksum: binary.LittleEndian.Uint32(data[8:12]),
		Index:    binary.LittleEndian.Uint64(data[12:20]),
	}
}

func (e *EntryHeader) ToBytes() []byte {
	b := make([]byte, EntryHeaderSize)
	e.PutBytes(b)
	return b
}

func (e *EntryHeader) PutBytes(b []byte) {
	binary.LittleEndian.PutUint64(b[0:8], e.Length)
	binary.LittleEndian.PutUint32(b[8:12], e.Checksum)
	binary.LittleEndian.PutUint64(b[12:20], e.Index)
}
