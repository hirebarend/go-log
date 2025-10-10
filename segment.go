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
	File           *os.File
	Cache          []uint64
	CommittedIndex uint64
	EndIndex       uint64
	mu             sync.RWMutex
	Name           string
	Size           uint64
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
		CommittedIndex: startIndex,
		EndIndex:       0,
		File:           nil,
		Name:           name,
		Size:           0,
		StartIndex:     startIndex,
		Writer:         nil,
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
	}

	s.Writer = nil

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

	committedIndex := s.EndIndex

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
	if committedIndex > s.CommittedIndex {
		s.CommittedIndex = committedIndex
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
		if err := s.File.Sync(); err != nil {
			return err
		}

		if err := s.File.Close(); err != nil {
			return err
		}

		s.File = nil
	}

	if err := os.Remove(s.Name); err != nil {
		return err
	}

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

func (s *Segment) Read(index uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.open(); err != nil {
		return nil, err
	}

	if s.Writer != nil {
		if err := s.Writer.Flush(); err != nil {
			return nil, err
		}
	}

	if index < s.StartIndex || index > s.EndIndex {
		return nil, fmt.Errorf("read index %d out of segment range [%d,%d]", index, s.StartIndex, s.EndIndex)
	}

	if s.Cache != nil {
		i := int(index - s.StartIndex)

		if i < 0 || i >= len(s.Cache) {
			return nil, fmt.Errorf("index %d not found in segment", index)
		}

		offset := s.Cache[i]

		entry, err := s.readEntryAtOffset(offset)

		if err != nil {
			return nil, err
		}

		return entry.Data, nil
	}

	var offset uint64 = 0

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
			offset = offset + EntryHeaderSize + entryHeader.Length
		}
	}
}

func (s *Segment) Truncate(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.open(); err != nil {
		return err
	}

	if err := s.Writer.Flush(); err != nil {
		return err
	}

	if index < s.StartIndex || index > s.EndIndex {
		return fmt.Errorf("truncate index %d out of segment range [%d,%d]", index, s.StartIndex, s.EndIndex)
	}

	var offset uint64 = 0

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

			s.Size = offset

			if offset == 0 {
				s.EndIndex = 0
				s.CommittedIndex = 0

				if s.Cache != nil {
					s.Cache = []uint64{}
				}
			} else {
				if entryHeader.Index > 0 {
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
				} else {
					s.CommittedIndex = 0
					s.EndIndex = 0

					if s.Cache != nil {
						s.Cache = []uint64{}
					}
				}
			}

			s.Writer.Reset(s.File)

			return nil
		}

		offset = offset + EntryHeaderSize + entryHeader.Length
	}

	return nil
}

func (s *Segment) Write(data []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.open(); err != nil {
		return 0, err
	}

	index := uint64(0)

	if s.EndIndex == 0 {
		index = s.StartIndex
	} else {
		index = s.EndIndex + 1
	}

	entry, err := NewEntry(data, index)

	if err != nil {
		return 0, err
	}

	b, err := entry.ToBytes()

	if err != nil {
		return 0, err
	}

	if _, err := s.Writer.Write(b); err != nil {
		return 0, err
	}

	if s.Cache != nil {
		s.Cache = append(s.Cache, s.Size)
	}

	s.EndIndex = entry.Header.Index
	s.Size += uint64(len(b))

	return entry.Header.Index, nil
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

		if s.Size != 0 {
			var offset uint64 = 0

			for {
				header, err := s.readEntryHeaderAtOffset(offset)

				if err == io.EOF {
					break
				}

				if err != nil {
					return err
				}

				s.CommittedIndex = header.Index
				s.EndIndex = header.Index

				if s.Cache != nil {
					s.Cache = append(s.Cache, offset)
				}

				offset = offset + EntryHeaderSize + header.Length
			}
		}
	}

	if s.Writer == nil {
		s.Writer = bufio.NewWriter(s.File)
	}

	return nil
}

func (s *Segment) readEntryAtOffset(offset uint64) (*Entry, error) {
	entryHeader, err := s.readEntryHeaderAtOffset(offset)

	if err != nil {
		return nil, err
	}

	data := make([]byte, entryHeader.Length)
	n, err := s.File.ReadAt(data, int64(offset+EntryHeaderSize))

	if err != nil {
		if err == io.EOF && n == int(entryHeader.Length) {
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

func (s *Segment) readEntryHeaderAtOffset(offset uint64) (*EntryHeader, error) {
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
