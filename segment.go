package golog

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Segment struct {
	File       *os.File
	IndexEnd   uint64
	IndexStart uint64
	Size       uint64
	Writer     *bufio.Writer
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
		File:       file,
		IndexEnd:   0,
		IndexStart: indexStart,
		Size:       uint64(stat.Size()),
		Writer:     bufio.NewWriter(file),
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

			segment.IndexEnd = entry.Header.Index

			offset = offset + EntryHeaderSize + entry.Header.Length
		}
	}

	return segment, nil
}

func (s *Segment) Close() error {
	if err := s.Commit(); err != nil {
		return err
	}

	s.Writer = nil

	if err := s.File.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Segment) Commit() error {
	if s.File == nil {
		return errors.New("File is not initialized")
	}

	if s.Writer == nil {
		return errors.New("Writer is not initialized")
	}

	if err := s.Writer.Flush(); err != nil {
		return err
	}

	if err := s.File.Sync(); err != nil {
		return err
	}

	return nil
}

func (s *Segment) Delete() error {
	if s.File == nil {
		return errors.New("File is not initialized")
	}

	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.File.Name()); err != nil {
		return err
	}

	return nil
}

func (s *Segment) ReadAll() []*Entry {
	entries := []*Entry{}

	if s.Size != 0 {
		var offset uint64 = 0

		for {
			entry, err := s.readEntry(offset)

			if err == io.EOF {
				break
			}

			if err != nil {
				break
			}

			entries = append(entries, entry)

			offset = offset + EntryHeaderSize + entry.Header.Length
		}
	}

	return entries
}

func (s *Segment) Truncate(idx uint64) error {
	if err := s.Commit(); err != nil {
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
	if s.Writer == nil {
		return nil, errors.New("Writer is not initialized")
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

func (s *Segment) readEntry(offset uint64) (*Entry, error) {
	if err := s.Commit(); err != nil {
		return nil, err
	}

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
	if err := s.Commit(); err != nil {
		return nil, err
	}

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
