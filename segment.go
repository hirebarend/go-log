package golog

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Segment struct {
	file       *os.File
	indexEnd   uint64
	indexStart uint64
	size       uint64
	writer     *bufio.Writer
}

func NewSegment(filename string) (*Segment, error) {
	base := filepath.Base(filename)

	indexStartStr := strings.TrimSuffix(base, filepath.Ext(base))

	indexStart, err := strconv.ParseUint(indexStartStr, 10, 64)

	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0o644)

	if err != nil {
		return nil, err
	}

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	stat, err := file.Stat()

	if err != nil {
		return nil, err
	}

	segment := &Segment{
		file:       file,
		indexEnd:   0,
		indexStart: indexStart,
		size:       uint64(stat.Size()),
		writer:     bufio.NewWriter(file),
	}

	if stat.Size() != 0 {
		var offset uint64 = 0

		for {
			entry, err := segment.readEntry(offset)

			if err == io.EOF {
				break
			}

			if err != nil {
				break
			}

			segment.indexEnd = entry.Header.Index

			offset = offset + EntryHeaderSize + entry.Header.Length
		}
	}

	return segment, nil
}

func (s *Segment) Write(data []byte) (*Entry, error) {
	entry, err := NewEntry(data, s.indexEnd+1)

	if err != nil {
		return nil, err
	}

	b, err := entry.ToBytes()

	if err != nil {
		return nil, err
	}

	if _, err := s.writer.Write(b); err != nil {
		return nil, err
	}

	if err := s.writer.Flush(); err != nil {
		return nil, err
	}

	// if err := s.file.Sync(); err != nil {
	// 	return nil, err
	// }

	s.indexEnd = entry.Header.Index
	s.size += uint64(len(b))

	return entry, nil
}

func (s *Segment) readEntry(offset uint64) (*Entry, error) {
	if s.writer != nil {
		if err := s.writer.Flush(); err != nil {
			return nil, err
		}
	}

	entryHeader, err := s.readEntryHeader(offset)

	if err != nil {
		return nil, err
	}

	data := make([]byte, entryHeader.Length)
	n, err := s.file.ReadAt(data, int64(offset+EntryHeaderSize))

	if err != nil {
		if err == io.EOF && n == int(entryHeader.Length) {
			// exactly at end; ok
		} else {
			return nil, err
		}
	}

	if n != int(entryHeader.Length) {
		return nil, io.ErrUnexpectedEOF
	}

	if entryHeader.Checksum != crc32.ChecksumIEEE(data) {
		return nil, fmt.Errorf("checksum mismatch at offset %d (index %d)", offset, entryHeader.Index)
	}

	return &Entry{
		Header: *entryHeader,
		Data:   data,
	}, nil
}

func (s *Segment) readEntryHeader(offset uint64) (*EntryHeader, error) {
	if s.writer != nil {
		if err := s.writer.Flush(); err != nil {
			return nil, err
		}
	}

	if offset >= s.size {
		return nil, io.EOF
	}

	data := make([]byte, EntryHeaderSize)
	n, err := s.file.ReadAt(data, int64(offset))

	if err != nil {
		if err == io.EOF && n == 0 {
			return nil, io.EOF
		}

		if n < int(EntryHeaderSize) {
			return nil, io.ErrUnexpectedEOF
		}
	}

	if n < int(EntryHeaderSize) {
		return nil, io.ErrUnexpectedEOF
	}

	entryHeader := NewEntryHeaderFromBytes(data)

	return &entryHeader, nil
}
