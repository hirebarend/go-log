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

// func load() {
// 	entries, err := os.ReadDir(l.dir)
// }

// func (l *Log) Get(index uint64) (LogEntry, bool) {
// 	if len(l.logEntries) == 0 {
// 		return LogEntry{}, false
// 	}

// 	lo, hi := 0, len(l.logEntries)-1

// 	for lo <= hi {
// 		mid := (lo + hi) / 2
// 		logEntry := l.logEntries[mid]

// 		if logEntry.Index == index {
// 			return logEntry, true
// 		}

// 		if logEntry.Index < index {
// 			lo = mid + 1
// 		} else {
// 			hi = mid - 1
// 		}
// 	}

// 	return LogEntry{}, false
// }

// func (l *Log) GetTerm(index uint64) (uint64, bool) {
// 	logEntry, ok := l.Get(index)

// 	if !ok {
// 		return 0, false
// 	}

// 	return logEntry.Term, true
// }

// func (l *Log) Last() (LogEntry, bool) {
// 	if len(l.logEntries) == 0 {
// 		return LogEntry{}, false
// 	}

// 	return l.logEntries[len(l.logEntries)-1], true
// }

// func (l *Log) LastIndex() (uint64, bool) {
// 	logEntry, ok := l.Last()

// 	if !ok {
// 		return 0, false
// 	}

// 	return logEntry.Index, true
// }

// func (l *Log) LastTerm() (uint64, bool) {
// 	logEntry, ok := l.Last()

// 	if !ok {
// 		return 0, false
// 	}

// 	return logEntry.Term, true
// }

// func (l *Log) Truncate(index uint64) {
// 	if len(l.logEntries) == 0 {
// 		return
// 	}

// 	position := -1

// 	for i, logEntry := range l.logEntries {
// 		if logEntry.Index == index {
// 			position = i
// 			break
// 		}
// 	}

// 	if position == -1 {
// 		return
// 	}

// 	l.logEntries = l.logEntries[:position]
// }
