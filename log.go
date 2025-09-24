package golog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type Log struct {
	Dir      string
	Segments []*Segment
}

func NewLog(dir string) *Log {
	return &Log{
		Dir:      dir,
		Segments: []*Segment{},
	}
}

func (l *Log) Commit() error {
	segment := l.Segments[len(l.Segments)-1]

	return segment.Commit()
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
		return segments[i].IndexEnd < segments[j].IndexEnd
	})

	l.Segments = segments

	if len(l.Segments) == 0 {
		segment, err := NewSegment(filepath.Join(l.Dir, fmt.Sprintf("%020d.seg", 1)))

		if err != nil {
			return err
		}

		l.Segments = append(l.Segments, segment)
	}

	return err
}

func (l *Log) TruncateFrom(index uint64) error {
	if len(l.Segments) == 0 {
		return nil
	}

	cut := len(l.Segments)

	for i := len(l.Segments) - 1; i >= 0; i-- {
		segment := l.Segments[i]

		if index < segment.IndexStart {
			if segment.Writer != nil {
				if err := segment.Writer.Flush(); err != nil {
					return err
				}
			}

			if err := segment.File.Close(); err != nil {
				return err
			}

			if err := os.Remove(segment.File.Name()); err != nil {
				return err
			}

			cut = i

			continue
		}

		if index >= segment.IndexStart && index <= segment.IndexEnd {
			if err := segment.Truncate(index); err != nil {
				return err
			}

			cut = i + 1

			break
		}

		if index > segment.IndexEnd {
			cut = i + 1

			break
		}
	}

	l.Segments = l.Segments[:cut]

	return nil
}

func (l *Log) TruncateTo(index uint64) error {
	if len(l.Segments) == 0 {
		return nil
	}

	var segments []*Segment

	for i, segment := range l.Segments {
		if segment.IndexEnd < index {
			if segment.Writer != nil {
				if err := segment.Writer.Flush(); err != nil {
					return err
				}
			}

			if err := segment.File.Close(); err != nil {
				return err
			}

			if err := os.Remove(segment.File.Name()); err != nil {
				return err
			}

			continue
		}

		if segment.IndexStart >= index {
			segments = append(segments, segment)

			continue
		}

		var offset uint64 = 0
		var newSegment *Segment

		for {
			entry, err := segment.readEntry(offset)

			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}

			if entry.Header.Index >= index {
				if newSegment == nil {
					ns, err := NewSegment(filepath.Join(l.Dir, fmt.Sprintf("%020d.seg", entry.Header.Index)))

					if err != nil {
						return err
					}

					newSegment = ns
				}

				if _, err := newSegment.Write(entry.Data); err != nil {
					return err
				}
			}

			offset = offset + EntryHeaderSize + entry.Header.Length
		}

		if newSegment != nil {
			if err := newSegment.Commit(); err != nil {
				return err
			}
		}

		if segment.Writer != nil {
			if err := segment.Writer.Flush(); err != nil {
				return err
			}
		}

		if err := segment.File.Close(); err != nil {
			return err
		}

		if err := os.Remove(segment.File.Name()); err != nil {
			return err
		}

		segments = append(segments, newSegment)
		segments = append(segments, l.Segments[i+1:]...)

		l.Segments = segments

		return nil
	}

	l.Segments = segments

	return nil
}

func (l *Log) Write(data []byte) (uint64, error) {
	if len(l.Segments) == 0 {
		return 0, fmt.Errorf("no segment found")
	}

	segment := l.Segments[len(l.Segments)-1]

	size := uint64(4 + 4 + 8 + len(data))

	if segment.Size+size > 25*1_000_000 {
		newSegment, err := NewSegment(filepath.Join(l.Dir, fmt.Sprintf("%020d.seg", segment.IndexEnd+1)))

		if err != nil {
			return 0, err
		}

		l.Segments = append(l.Segments, newSegment)

		if err := segment.Commit(); err != nil {
			return 0, err
		}

		segment = l.Segments[len(l.Segments)-1]
	}

	entry, err := segment.Write(data)

	if err != nil {
		return 0, err
	}

	return entry.Header.Index, nil
}
