package golog

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type Log[T any] struct {
	Dir            string
	mu             sync.RWMutex
	MaxSegmentSzie uint64
	Segments       []*Segment
}

func NewLog[T any](dir string, maxSegmentSize uint64) *Log[T] {
	return &Log[T]{
		Dir:            dir,
		MaxSegmentSzie: maxSegmentSize,
		Segments:       []*Segment{},
	}
}

func (l *Log[T]) Close() error {
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

func (l *Log[T]) Commit() error {
	l.mu.RLock()

	if len(l.Segments) == 0 {
		l.mu.RUnlock()

		return nil
	}

	segment := l.Segments[len(l.Segments)-1]

	l.mu.RUnlock()

	return segment.Commit()
}

func (l *Log[T]) GetCommittedIndex() (uint64, error) {
	l.mu.RLock()

	if len(l.Segments) == 0 {
		l.mu.RUnlock()

		return 0, nil
	}

	segment := l.Segments[len(l.Segments)-1]

	l.mu.RUnlock()

	return segment.CommittedIndex, nil
}

func (l *Log[T]) GetLastIndex() (uint64, error) {
	l.mu.RLock()

	if len(l.Segments) == 0 {
		l.mu.RUnlock()

		return 0, nil
	}

	segment := l.Segments[len(l.Segments)-1]

	l.mu.RUnlock()

	return segment.EndIndex, nil
}

func (l *Log[T]) Load() error {
	dirEntries, err := os.ReadDir(l.Dir)

	if err != nil {
		return err
	}

	var segments []*Segment

	for _, dirEntry := range dirEntries {
		if !strings.HasSuffix(dirEntry.Name(), ".seg") {
			continue
		}

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

func (l *Log[T]) Read(index uint64) ([]byte, error) {
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

func (l *Log[T]) ReadAndDeserialize(index uint64) (*T, error) {
	data, err := l.Read(index)

	if err != nil {
		return nil, err
	}

	var result T

	buf := bytes.NewBuffer(data)

	dec := gob.NewDecoder(buf)

	err = dec.Decode(&result)

	if err != nil {
		return nil, err
	}

	return &result, err
}

func (l *Log[T]) SerializeAndWrite(obj *T) (uint64, error) {
	var data bytes.Buffer

	encoder := gob.NewEncoder(&data)

	err := encoder.Encode(obj)

	if err != nil {
		return 0, err
	}

	index, err := l.Write(data.Bytes())

	if err != nil {
		return 0, err
	}

	return index, nil
}

func (l *Log[T]) TruncateFrom(index uint64) error {
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

func (l *Log[T]) TruncateTo(index uint64) error {
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

func (l *Log[T]) Write(data []byte) (uint64, error) {
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

	l.mu.Unlock()

	index, err := segment.Write(data)

	if err != nil {
		return 0, err
	}

	return index, nil
}
