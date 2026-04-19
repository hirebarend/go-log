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
	"sync/atomic"
)

const segmentBufferSize = 64 * 1024

type Segment struct {
	File           *os.File
	Cache          []uint64
	CommittedIndex uint64
	EndIndex       uint64
	dirty          bool
	mu             sync.RWMutex
	Name           string
	size           atomic.Uint64
	StartIndex     uint64
	Writer         *bufio.Writer
}

func NewSegment(name string) (*Segment, error) {
	base := filepath.Base(name)
	startIndexStr := strings.TrimSuffix(base, filepath.Ext(base))

	startIndex, err := strconv.ParseUint(startIndexStr, 10, 64)
	if err != nil {
		return nil, err
	}

	segment := &Segment{
		Cache:          []uint64{},
		CommittedIndex: 0,
		EndIndex:       0,
		Name:           name,
		StartIndex:     startIndex,
	}

	if err := segment.open(); err != nil {
		return nil, err
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
		s.Writer = nil
	}

	if s.File != nil {
		if err := s.File.Sync(); err != nil {
			return err
		}
		if err := s.File.Close(); err != nil {
			return err
		}
		s.File = nil
	}

	s.Cache = nil
	return nil
}

func (s *Segment) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	committedIndex := s.EndIndex

	if err := s.open(); err != nil {
		return err
	}

	if s.Writer != nil {
		if err := s.Writer.Flush(); err != nil {
			return err
		}
		s.dirty = false
	}

	if s.File != nil {
		if err := fdatasync(s.File); err != nil {
			return err
		}
	}

	if committedIndex > s.CommittedIndex {
		s.CommittedIndex = committedIndex
	}

	return nil
}

func (s *Segment) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Writer != nil {
		if err := s.Writer.Flush(); err != nil {
			return err
		}
		s.Writer = nil
	}

	if s.File != nil {
		if err := s.File.Sync(); err != nil {
			return err
		}
		if err := s.File.Close(); err != nil {
			return err
		}
		s.File = nil
	}

	return os.Remove(s.Name)
}

func (s *Segment) Read(index uint64) ([]byte, error) {
	s.mu.Lock()

	if err := s.open(); err != nil {
		s.mu.Unlock()
		return nil, err
	}

	if s.dirty && s.Writer != nil {
		if err := s.Writer.Flush(); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		s.dirty = false
	}

	s.mu.Unlock()

	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.StartIndex || index > s.EndIndex {
		return nil, fmt.Errorf("read index %d out of segment range [%d,%d]", index, s.StartIndex, s.EndIndex)
	}

	if s.Cache != nil {
		i := int(index - s.StartIndex)
		if i < 0 || i >= len(s.Cache) {
			return nil, fmt.Errorf("index %d not found in segment", index)
		}

		entry, err := s.readEntryAtOffset(s.Cache[i])
		if err != nil {
			return nil, err
		}
		return entry.Data, nil
	}

	var offset uint64
	for {
		entryHeader, err := s.readEntryHeaderAtOffset(offset)
		if err == io.EOF {
			return nil, fmt.Errorf("index %d not found in segment", index)
		}
		if err != nil {
			return nil, err
		}

		switch {
		case entryHeader.Index == index:
			entry, err := s.readEntryAtOffset(offset)
			if err != nil {
				return nil, err
			}
			return entry.Data, nil
		case entryHeader.Index > index:
			return nil, fmt.Errorf("index %d not found in segment", index)
		default:
			offset += EntryHeaderSize + entryHeader.Length
		}
	}
}

func (s *Segment) Truncate(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.open(); err != nil {
		return err
	}

	if s.Writer != nil {
		if err := s.Writer.Flush(); err != nil {
			return err
		}
		s.dirty = false
	}

	if index < s.StartIndex || index > s.EndIndex {
		return fmt.Errorf("truncate index %d out of segment range [%d,%d]", index, s.StartIndex, s.EndIndex)
	}

	var offset uint64
	for {
		entryHeader, err := s.readEntryHeaderAtOffset(offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if entryHeader.Index >= index {
			if err := s.File.Truncate(int64(offset)); err != nil {
				return err
			}
			if _, err := s.File.Seek(0, io.SeekEnd); err != nil {
				return err
			}

			s.size.Store(offset)

			if offset == 0 {
				s.EndIndex = 0
				s.CommittedIndex = 0
				if s.Cache != nil {
					s.Cache = s.Cache[:0]
				}
			} else {
				s.CommittedIndex = entryHeader.Index - 1
				s.EndIndex = entryHeader.Index - 1
				if s.Cache != nil {
					newLen := int(s.EndIndex - s.StartIndex + 1)
					if newLen < 0 {
						newLen = 0
					}
					if newLen > len(s.Cache) {
						newLen = len(s.Cache)
					}
					s.Cache = s.Cache[:newLen]
				}
			}

			if s.Writer != nil {
				s.Writer.Reset(s.File)
			}
			return nil
		}

		offset += EntryHeaderSize + entryHeader.Length
	}

	return nil
}

func (s *Segment) Write(data []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.open(); err != nil {
		return 0, err
	}

	if s.Writer == nil {
		s.Writer = bufio.NewWriterSize(s.File, segmentBufferSize)
	}

	var index uint64
	if s.EndIndex == 0 {
		index = s.StartIndex
	} else {
		index = s.EndIndex + 1
	}

	header := EntryHeader{
		Length:   uint64(len(data)),
		Checksum: crc32.ChecksumIEEE(data),
		Index:    index,
	}

	// Write header directly to bufio.Writer using a stack-allocated buffer.
	var hdrBuf [EntryHeaderSize]byte
	header.PutBytes(hdrBuf[:])
	if _, err := s.Writer.Write(hdrBuf[:]); err != nil {
		return 0, err
	}
	if _, err := s.Writer.Write(data); err != nil {
		return 0, err
	}

	if s.Cache != nil {
		s.Cache = append(s.Cache, s.size.Load())
	}

	s.dirty = true
	s.EndIndex = index
	s.size.Add(EntryHeaderSize + uint64(len(data)))

	return index, nil
}

func (s *Segment) open() error {
	if s.File == nil {
		directory := filepath.Dir(s.Name)
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", directory, err)
		}

		file, err := os.OpenFile(s.Name, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return err
		}

		if _, err := file.Seek(0, io.SeekEnd); err != nil {
			file.Close()
			return err
		}

		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return err
		}

		s.File = file
		s.size.Store(uint64(stat.Size()))

		if s.size.Load() != 0 {
			// Use a buffered reader for sequential header scanning instead of
			// individual pread syscalls per entry.
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				return err
			}
			br := bufio.NewReaderSize(file, segmentBufferSize)
			var offset uint64
			var hdrBuf [EntryHeaderSize]byte
			for {
				if offset >= s.size.Load() {
					break
				}
				if _, err := io.ReadFull(br, hdrBuf[:]); err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						break
					}
					return err
				}
				header := NewEntryHeaderFromBytes(hdrBuf[:])

				s.CommittedIndex = header.Index
				s.EndIndex = header.Index

				if s.Cache != nil {
					s.Cache = append(s.Cache, offset)
				}

				skip := int64(header.Length)
				if _, err := br.Discard(int(skip)); err != nil {
					return err
				}
				offset += EntryHeaderSize + header.Length
			}

			// Seek back to end for future writes.
			if _, err := file.Seek(0, io.SeekEnd); err != nil {
				return err
			}
		}
	}

	// Writer is created lazily on first Write, not here.

	return nil
}

func (s *Segment) readEntryAtOffset(offset uint64) (*Entry, error) {
	entryHeader, err := s.readEntryHeaderAtOffset(offset)
	if err != nil {
		return nil, err
	}

	data := make([]byte, entryHeader.Length)
	n, err := s.File.ReadAt(data, int64(offset+EntryHeaderSize))
	if err != nil && !(err == io.EOF && n == int(entryHeader.Length)) {
		return nil, err
	}
	if n != int(entryHeader.Length) {
		return nil, io.ErrUnexpectedEOF
	}

	if entryHeader.Checksum != crc32.ChecksumIEEE(data) {
		return nil, fmt.Errorf("checksum mismatch at offset %d (index %d)", offset, entryHeader.Index)
	}

	return &Entry{
		Header: entryHeader,
		Data:   data,
	}, nil
}

func (s *Segment) readEntryHeaderAtOffset(offset uint64) (EntryHeader, error) {
	if offset >= s.size.Load() {
		return EntryHeader{}, io.EOF
	}

	var buf [EntryHeaderSize]byte
	n, err := s.File.ReadAt(buf[:], int64(offset))
	if err != nil {
		if err == io.EOF && n == 0 {
			return EntryHeader{}, io.EOF
		}
		if n < EntryHeaderSize {
			return EntryHeader{}, io.ErrUnexpectedEOF
		}
	}
	if n < EntryHeaderSize {
		return EntryHeader{}, io.ErrUnexpectedEOF
	}

	return NewEntryHeaderFromBytes(buf[:]), nil
}
