package golog

import "encoding/binary"

const EntryHeaderSize = 8 + 4 + 8

type EntryHeader struct {
	Length   uint64
	Checksum uint32
	Index    uint64
}

func NewEntryHeaderFromBytes(data []byte) EntryHeader {
	length := binary.LittleEndian.Uint64(data[0:8])
	checksum := binary.LittleEndian.Uint32(data[8:12])
	index := binary.LittleEndian.Uint64(data[12:20])

	return EntryHeader{
		Length:   length,
		Checksum: checksum,
		Index:    index,
	}
}

func (e *EntryHeader) ToBytes() []byte {
	b := make([]byte, EntryHeaderSize)

	binary.LittleEndian.PutUint64(b[0:8], e.Length)
	binary.LittleEndian.PutUint32(b[8:12], e.Checksum)
	binary.LittleEndian.PutUint64(b[12:20], e.Index)

	return b
}
