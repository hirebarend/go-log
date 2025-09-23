package golog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type Log struct {
	dir      string
	segments []*Segment
}

func NewLog(dir string) *Log {
	return &Log{
		dir:      dir,
		segments: []*Segment{},
	}
}

func (l *Log) Load() error {
	dirEntries, err := os.ReadDir(l.dir)

	if err != nil {
		return err
	}

	var segments []*Segment

	for _, dirEntry := range dirEntries {

		segment, err := NewSegment(filepath.Join(l.dir, dirEntry.Name()))

		if err != nil {
			return err
		}

		segments = append(segments, segment)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].indexEnd < segments[j].indexEnd
	})

	l.segments = segments

	if len(l.segments) == 0 {
		segment, err := NewSegment(filepath.Join(l.dir, fmt.Sprintf("%020d.seg", 0)))

		if err != nil {
			return err
		}

		l.segments = append(l.segments, segment)
	}

	return err
}

func (l *Log) Write(data []byte) (uint64, error) {
	if len(l.segments) == 0 {
		return 0, fmt.Errorf("no segment found")
	}

	segment := l.segments[len(l.segments)-1]

	size := uint64(4 + 4 + 8 + len(data))

	// TODO:
	if segment.size+size > 25165824 {
		newSegment, err := NewSegment(filepath.Join(l.dir, fmt.Sprintf("%020d.seg", segment.indexEnd)))

		if err != nil {
			return 0, err
		}

		l.segments = append(l.segments, newSegment)

		segment = newSegment
	}

	entry, err := segment.Write(data)

	if err != nil {
		return 0, err
	}

	return entry.Header.Index, nil
}
