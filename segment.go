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
	"sync"
)

type Segment struct {
	File          *os.File
	IndexCommited uint64
	IndexEnd      uint64
	IndexStart    uint64
	mu            sync.Mutex
	Name          string
	Size          uint64
	Writer        *bufio.Writer
}

func NewSegment(name string) (*Segment, error) {
	base := filepath.Base(name)

	indexStartStr := strings.TrimSuffix(base, filepath.Ext(base))

	indexStart, err := strconv.ParseUint(indexStartStr, 10, 64)

	if err != nil {
		return nil, err
	}

	segment := &Segment{
		File:          nil,
		IndexCommited: indexStart,
		IndexEnd:      0,
		IndexStart:    indexStart,
		Name:          name,
		Size:          0,
		Writer:        nil,
	}

	if err := segment.open(); err != nil {
		return nil, err
	}

	if segment.Size != 0 {
		var offset uint64 = 0

		for {
			entry, err := segment.readEntry(offset)

			if err == io.EOF {
				break
			}

			if err != nil {
				break
			}

			segment.IndexCommited = entry.Header.Index
			segment.IndexEnd = entry.Header.Index

			offset = offset + EntryHeaderSize + entry.Header.Length
		}
	}

	return segment, nil
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Writer != nil {
		if err := s.Writer.Flush(); err != nil {
			return err
		}
	}

	s.Writer = nil

	if s.File != nil {
		if err := s.File.Close(); err != nil {
			return err
		}

		s.File = nil
	}

	s.IndexCommited = s.IndexStart
	s.IndexEnd = 0
	s.Size = 0

	return nil
}

func (s *Segment) Commit() error {
	s.mu.Lock()

	indexCommitted := s.IndexEnd

	if err := s.open(); err != nil {
		s.mu.Unlock()

		return err
	}

	if s.Writer != nil {
		if err := s.Writer.Flush(); err != nil {
			s.mu.Unlock()

			return err
		}
	}

	s.mu.Unlock()

	if err := s.File.Sync(); err != nil {
		return err
	}

	s.mu.Lock()
	if indexCommitted > s.IndexCommited {
		s.IndexCommited = indexCommitted
	}
	s.mu.Unlock()

	return nil
}

func (s *Segment) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Writer != nil {
		if err := s.Writer.Flush(); err != nil {
			return err
		}
	}

	s.Writer = nil

	if s.File != nil {
		if err := s.File.Close(); err != nil {
			return err
		}

		s.File = nil
	}

	if err := os.Remove(s.Name); err != nil {
		return err
	}

	s.Size = 0

	return nil
}

func (s *Segment) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.open(); err != nil {
		return err
	}

	return nil
}

func (s *Segment) Truncate(idx uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.open(); err != nil {
		return err
	}

	if err := s.Writer.Flush(); err != nil {
		return err
	}

	if idx < s.IndexStart || idx > s.IndexEnd {
		return fmt.Errorf("truncate idx %d out of segment range [%d,%d]", idx, s.IndexStart, s.IndexEnd)
	}

	var offset uint64 = 0

	for {
		entryHeader, err := s.readEntryHeader(offset)

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if entryHeader.Index > idx {
			if err := s.File.Truncate(int64(offset)); err != nil {
				return err
			}

			if _, err := s.File.Seek(0, io.SeekEnd); err != nil {
				return err
			}

			s.Size = offset

			if entryHeader.Index > 0 {
				s.IndexEnd = entryHeader.Index - 1
			} else {
				s.IndexEnd = 0
			}

			s.Writer.Reset(s.File)

			return nil
		}

		offset = offset + EntryHeaderSize + entryHeader.Length
	}

	return nil
}

func (s *Segment) Write(data []byte) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.open(); err != nil {
		return nil, err
	}

	index := uint64(0)

	if s.IndexEnd == 0 {
		index = s.IndexStart
	} else {
		index = s.IndexEnd + 1
	}

	entry, err := NewEntry(data, index)

	if err != nil {
		return nil, err
	}

	b, err := entry.ToBytes()

	if err != nil {
		return nil, err
	}

	if _, err := s.Writer.Write(b); err != nil {
		return nil, err
	}

	s.IndexEnd = entry.Header.Index
	s.Size += uint64(len(b))

	return entry, nil
}

func (s *Segment) open() error {
	if s.File == nil {
		file, err := os.OpenFile(s.Name, os.O_CREATE|os.O_RDWR, 0o644)

		if err != nil {
			return err
		}

		if _, err := file.Seek(0, io.SeekEnd); err != nil {
			return err
		}

		stat, err := file.Stat()

		if err != nil {
			return err
		}

		s.File = file
		s.Size = uint64(stat.Size())
	}

	if s.Writer == nil {
		s.Writer = bufio.NewWriter(s.File)
	}

	return nil
}

func (s *Segment) readEntry(offset uint64) (*Entry, error) {
	entryHeader, err := s.readEntryHeader(offset)

	if err != nil {
		return nil, err
	}

	data := make([]byte, entryHeader.Length)
	n, err := s.File.ReadAt(data, int64(offset+EntryHeaderSize))

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
	if offset >= s.Size {
		return nil, io.EOF
	}

	data := make([]byte, EntryHeaderSize)
	n, err := s.File.ReadAt(data, int64(offset))

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
