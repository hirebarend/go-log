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
	"time"
)

type Waiter struct {
	done chan error
}

type Log[T any] struct {
	Dir            string
	mu             sync.RWMutex
	MaxSegmentSize uint64
	Segments       []*Segment

	stopCh    chan struct{}
	stopped   chan struct{}
	waiters   []Waiter
	waitersMu sync.Mutex
}

func NewLog[T any](dir string, maxSegmentSize uint64) *Log[T] {
	log := &Log[T]{
		Dir:            dir,
		MaxSegmentSize: maxSegmentSize,
		Segments:       []*Segment{},

		stopCh:  make(chan struct{}),
		stopped: make(chan struct{}),
		waiters: []Waiter{},
	}

	go log.flusher()

	return log
}

func (l *Log[T]) Close() error {
	l.waitersMu.Lock()

	if len(l.waiters) > 0 {
		err := l.Commit()

		l.respondToWaiters(err)
	}

	l.waitersMu.Unlock()

	close(l.stopCh)
	<-l.stopped

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

func (l *Log[T]) ReadDeserialize(index uint64) (*T, error) {
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

func (l *Log[T]) SerializeWrite(obj *T) (uint64, error) {
	var data bytes.Buffer

	encoder := gob.NewEncoder(&data)

	err := encoder.Encode(obj)

	if err != nil {
		return 0, err
	}

	return l.Write(data.Bytes())
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
			l.mu.Unlock()

			return 0, err
		}

		l.Segments = append(l.Segments, segment)
	}

	segment := l.Segments[len(l.Segments)-1]

	size := uint64(EntryHeaderSize + len(data))

	if segment.Size+size > l.MaxSegmentSize {
		l.waitersMu.Lock()

		err := segment.Commit()

		l.respondToWaiters(err)

		l.waitersMu.Unlock()

		if err != nil {
			l.mu.Unlock()

			return 0, err
		}

		newSegment, err := NewSegment(filepath.Join(l.Dir, fmt.Sprintf("%020d.seg", segment.EndIndex+1)))

		if err != nil {
			l.mu.Unlock()

			return 0, err
		}

		l.Segments = append(l.Segments, newSegment)

		segment = l.Segments[len(l.Segments)-1]
	}

	l.mu.Unlock()

	return segment.Write(data)
}

func (l *Log[T]) WriteCommit(data []byte) (uint64, error) {
	index, err := l.Write(data)

	if err != nil {
		return 0, err
	}

	waiter := Waiter{done: make(chan error, 1)}

	l.waitersMu.Lock()
	l.waiters = append(l.waiters, waiter)
	l.waitersMu.Unlock()

	if err := <-waiter.done; err != nil {
		return 0, err
	}

	return index, nil
}

func (l *Log[T]) SerializeWriteCommit(obj *T) (uint64, error) {
	var data bytes.Buffer

	encoder := gob.NewEncoder(&data)

	err := encoder.Encode(obj)

	if err != nil {
		return 0, err
	}

	return l.WriteCommit(data.Bytes())
}

func (l *Log[T]) flusher() {
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()

	defer close(l.stopped)

	for {
		select {
		case <-l.stopCh:
			l.waitersMu.Lock()

			if len(l.waiters) > 0 {
				err := l.Commit()

				l.respondToWaiters(err)
			}

			l.waitersMu.Unlock()

			return
		case <-t.C:
			l.waitersMu.Lock()

			if len(l.waiters) == 0 {
				l.waitersMu.Unlock()

				continue
			}

			err := l.Commit()

			l.respondToWaiters(err)

			l.waitersMu.Unlock()
		}
	}
}

func (l *Log[T]) respondToWaiters(err error) {
	for _, w := range l.waiters {
		w.done <- err
	}

	l.waiters = []Waiter{}
}
