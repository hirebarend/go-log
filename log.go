package golog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type Log struct {
	Dir            string
	mu             sync.RWMutex
	MaxSegmentSzie uint64
	Segments       []*Segment
}

func NewLog(dir string, maxSegmentSize uint64) *Log {
	return &Log{
		Dir:            dir,
		MaxSegmentSzie: maxSegmentSize,
		Segments:       []*Segment{},
	}
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.Segments {
		err := segment.Close()

		if err != nil {
			return err
		}
	}

	l.Segments = []*Segment{}

	return nil
}

func (l *Log) Commit() error {
	l.mu.RLock()

	if len(l.Segments) == 0 {
		l.mu.RUnlock()

		return fmt.Errorf("no segment")
	}

	segment := l.Segments[len(l.Segments)-1]

	l.mu.RUnlock()

	return segment.Commit()
}

func (l *Log) GetCommittedIndex() (uint64, error) {
	l.mu.RLock()

	if len(l.Segments) == 0 {
		l.mu.RUnlock()

		return 0, fmt.Errorf("no segment")
	}

	segment := l.Segments[len(l.Segments)-1]

	l.mu.RUnlock()

	return segment.CommittedIndex, nil
}

func (l *Log) GetLastIndex() (uint64, error) {
	l.mu.RLock()

	if len(l.Segments) == 0 {
		l.mu.RUnlock()

		return 0, fmt.Errorf("no segment")
	}

	segment := l.Segments[len(l.Segments)-1]

	l.mu.RUnlock()

	return segment.EndIndex, nil
}

func (l *Log) Load() error {
	dirEntries, err := os.ReadDir(l.Dir)

	if err != nil {
		return err
	}

	var segments []*Segment

	for _, dirEntry := range dirEntries {
		segment, err := NewSegment(filepath.Join(l.Dir, dirEntry.Name()))

		if err != nil {
			return err
		}

		segments = append(segments, segment)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].StartIndex < segments[j].StartIndex
	})

	l.mu.Lock()
	l.Segments = segments
	l.mu.Unlock()

	return err
}

func (l *Log) Read(index uint64) ([]byte, error) {
	l.mu.RLock()

	if len(l.Segments) == 0 {
		l.mu.RUnlock()

		return nil, fmt.Errorf("no segment")
	}

	i := sort.Search(len(l.Segments), func(i int) bool { return l.Segments[i].StartIndex > index }) - 1

	if i == -1 {
		l.mu.RUnlock()

		return nil, fmt.Errorf("no segment")
	}

	segment := l.Segments[i]

	l.mu.RUnlock()

	if index < segment.StartIndex || index > segment.EndIndex {
		return nil, fmt.Errorf("no segment")
	}

	return segment.Read(index)
}

func (l *Log) TruncateFrom(index uint64) error {
	l.mu.Lock()

	if len(l.Segments) == 0 {
		l.mu.Unlock()

		return nil
	}

	cut := len(l.Segments)

	for i := len(l.Segments) - 1; i >= 0; i-- {
		segment := l.Segments[i]

		if index <= segment.StartIndex {
			if err := segment.Delete(); err != nil {
				l.mu.Unlock()

				return err
			}

			cut = i

			continue
		}

		if index >= segment.StartIndex && index <= segment.EndIndex {
			if err := segment.Truncate(index); err != nil {
				l.mu.Unlock()

				return err
			}

			cut = i + 1

			break
		}

		if index > segment.EndIndex {
			cut = i + 1

			break
		}
	}

	l.Segments = l.Segments[:cut]

	l.mu.Unlock()

	return nil
}

func (l *Log) TruncateTo(index uint64) error {
	l.mu.Lock()

	if len(l.Segments) == 0 {
		l.mu.Unlock()

		return nil
	}

	var segments []*Segment

	for i, segment := range l.Segments {
		if segment.EndIndex <= index {
			if err := segment.Delete(); err != nil {
				l.mu.Unlock()

				return err
			}

			continue
		}

		if segment.StartIndex > index {
			segments = append(segments, segment)

			continue
		}

		var offset uint64 = 0
		var newSegment *Segment

		for {
			entry, err := segment.readEntryAtOffset(offset)

			if err == io.EOF {
				break
			}

			if err != nil {
				l.mu.Unlock()

				return err
			}

			if entry.Header.Index > index {
				if newSegment == nil {
					ns, err := NewSegment(filepath.Join(l.Dir, fmt.Sprintf("%020d.seg", entry.Header.Index)))

					if err != nil {
						l.mu.Unlock()

						return err
					}

					newSegment = ns
				}

				if _, err := newSegment.Write(entry.Data); err != nil {
					l.mu.Unlock()

					return err
				}
			}

			offset = offset + EntryHeaderSize + entry.Header.Length
		}

		if newSegment != nil {
			if err := newSegment.Commit(); err != nil {
				l.mu.Unlock()

				return err
			}
		}

		if err := segment.Delete(); err != nil {
			l.mu.Unlock()

			return err
		}

		if newSegment != nil {
			segments = append(segments, newSegment)
		}

		segments = append(segments, l.Segments[i+1:]...)

		l.Segments = segments

		l.mu.Unlock()

		return nil
	}

	l.Segments = segments

	l.mu.Unlock()

	return nil
}

func (l *Log) Write(data []byte) (uint64, error) {
	l.mu.Lock()

	if len(l.Segments) == 0 {
		segment, err := NewSegment(filepath.Join(l.Dir, fmt.Sprintf("%020d.seg", 1)))

		if err != nil {
			return 0, err
		}

		l.Segments = append(l.Segments, segment)
	}

	segment := l.Segments[len(l.Segments)-1]

	size := uint64(EntryHeaderSize + len(data))

	if segment.Size+size > l.MaxSegmentSzie {
		if err := segment.Commit(); err != nil {
			return 0, err
		}

		newSegment, err := NewSegment(filepath.Join(l.Dir, fmt.Sprintf("%020d.seg", segment.EndIndex+1)))

		if err != nil {
			return 0, err
		}

		l.Segments = append(l.Segments, newSegment)

		segment = l.Segments[len(l.Segments)-1]
	}

	index, err := segment.Write(data)

	l.mu.Unlock()

	if err != nil {
		return 0, err
	}

	return index, nil
}
