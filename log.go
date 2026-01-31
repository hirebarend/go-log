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

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

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

	go log.flusher(10 * time.Millisecond)

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
	defer l.mu.RUnlock()

	if len(l.Segments) == 0 {
		return 0, nil
	}

	segment := l.Segments[len(l.Segments)-1]
	segment.mu.RLock()
	committedIndex := segment.CommittedIndex
	segment.mu.RUnlock()

	return committedIndex, nil
}

func (l *Log[T]) GetLastIndex() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.Segments) == 0 {
		return 0, nil
	}

	segment := l.Segments[len(l.Segments)-1]
	segment.mu.RLock()
	endIndex := segment.EndIndex
	segment.mu.RUnlock()

	return endIndex, nil
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
	l.closeOldSegments()
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
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	encoder := gob.NewEncoder(buf)

	if err := encoder.Encode(obj); err != nil {
		return 0, err
	}

	return l.Write(buf.Bytes())
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
	l.closeOldSegments()

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
			err := segment.open()

			if err != nil {
				l.mu.Unlock()

				return err
			}

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
		l.closeOldSegments()

		l.mu.Unlock()

		return nil
	}

	l.Segments = segments
	l.closeOldSegments()

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
		l.closeOldSegments()
	}

	segment := l.Segments[len(l.Segments)-1]

	size := uint64(EntryHeaderSize + len(data))

	if segment.size.Load()+size > l.MaxSegmentSize {
		l.mu.Unlock()

		l.waitersMu.Lock()

		err := segment.Commit()

		l.respondToWaiters(err)

		l.waitersMu.Unlock()

		if err != nil {
			return 0, err
		}

		l.mu.Lock()

		segment = l.Segments[len(l.Segments)-1]

		if segment.size.Load()+size > l.MaxSegmentSize {
			newSegment, err := NewSegment(filepath.Join(l.Dir, fmt.Sprintf("%020d.seg", segment.EndIndex+1)))

			if err != nil {
				l.mu.Unlock()

				return 0, err
			}

			l.Segments = append(l.Segments, newSegment)
			l.closeOldSegments()

			segment = l.Segments[len(l.Segments)-1]
		}
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

	select {
	case <-l.stopCh:
		l.waitersMu.Unlock()
		return 0, fmt.Errorf("log is closed")
	default:
	}

	l.waiters = append(l.waiters, waiter)
	l.waitersMu.Unlock()

	if err := <-waiter.done; err != nil {
		return 0, err
	}

	return index, nil
}

func (l *Log[T]) SerializeWriteCommit(obj *T) (uint64, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	encoder := gob.NewEncoder(buf)

	if err := encoder.Encode(obj); err != nil {
		return 0, err
	}

	return l.WriteCommit(buf.Bytes())
}

func (l *Log[T]) closeOldSegments() {
	if len(l.Segments) <= 5 {
		return
	}

	keepFrom := len(l.Segments) - 5

	for i := 0; i < keepFrom; i++ {
		_ = l.Segments[i].Close()
	}
}

func (l *Log[T]) flusher(d time.Duration) {
	t := time.NewTicker(d)
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

	l.waiters = l.waiters[:0]
}
