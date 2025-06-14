package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	outFileName    = "current-data"
	segmentPrefix  = "segment-"
	defaultMaxSize = 10 * 1024 * 1024
	workerPoolSize = 10
)

var ErrNotFound = fmt.Errorf("record does not exist")

type segment struct {
	id       int
	file     *os.File
	filePath string
	size     int64
	index    map[string]int64
}

type Db struct {
	mu         sync.RWMutex
	writeMutex sync.Mutex
	writeChan  chan writeRequest
	out        *segment
	segments   []*segment
	index      map[string]segmentLocation
	maxSize    int64
	dir        string
	nextSegID  int
	workerPool chan workerRequest
	writerDone chan struct{}
	closeOnce  sync.Once
}

type segmentLocation struct {
	segID  int
	offset int64
}

type workerRequest struct {
	key      string
	segID    int
	offset   int64
	filePath string
	result   chan workerResponse
}

type workerResponse struct {
	value string
	err   error
}

type writeRequest struct {
	key   string
	value string
	err   chan error
}

func Open(dir string) (*Db, error) {
	return OpenWithMaxSize(dir, defaultMaxSize)
}

func OpenWithMaxSize(dir string, maxSize int64) (*Db, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db := &Db{
		index:      make(map[string]segmentLocation),
		maxSize:    maxSize,
		dir:        dir,
		segments:   make([]*segment, 0),
		workerPool: make(chan workerRequest, workerPoolSize),
		writeChan:  make(chan writeRequest),
		writerDone: make(chan struct{}),
	}

	for i := 0; i < workerPoolSize; i++ {
		go db.worker()
	}

	go db.writer()

	if err := db.recover(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (db *Db) writer() {
	for {
		select {
		case req := <-db.writeChan:
			db.writeMutex.Lock()
			err := db.doPut(req.key, req.value)
			req.err <- err
			db.writeMutex.Unlock()
		case <-db.writerDone:
			return
		}
	}
}

func (db *Db) worker() {
	for req := range db.workerPool {
		file, err := os.Open(req.filePath)
		if err != nil {
			req.result <- workerResponse{err: err}
			continue
		}

		func() {
			defer file.Close()

			_, err = file.Seek(req.offset, 0)
			if err != nil {
				req.result <- workerResponse{err: err}
				return
			}

			var record entry
			_, err = record.DecodeFromReader(bufio.NewReader(file))
			if err != nil {
				req.result <- workerResponse{err: err}
				return
			}

			req.result <- workerResponse{value: record.value}
		}()
	}
}

func (db *Db) recover() error {
	files, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}
	var segFiles []struct {
		name string
		id   int
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var id int
		if file.Name() == outFileName {
			id = 0
		} else if _, err := fmt.Sscanf(file.Name(), segmentPrefix+"%d", &id); err != nil {
			continue
		}
		segFiles = append(segFiles, struct {
			name string
			id   int
		}{name: file.Name(), id: id})
	}

	sort.Slice(segFiles, func(i, j int) bool {
		return segFiles[i].id < segFiles[j].id
	})

	for _, sf := range segFiles {
		segPath := filepath.Join(db.dir, sf.name)
		f, err := os.OpenFile(segPath, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}

		seg := &segment{
			id:       sf.id,
			file:     f,
			filePath: segPath,
			size:     info.Size(),
			index:    make(map[string]int64),
		}

		db.segments = append(db.segments, seg)
		if sf.id >= db.nextSegID {
			db.nextSegID = sf.id + 1
		}

		if err := db.recoverSegmentIndex(seg); err != nil {
			return err
		}
	}

	if len(db.segments) == 0 {
		if err := db.createNewSegment(); err != nil {
			return err
		}
	} else {
		db.out = db.segments[len(db.segments)-1]
	}

	return nil
}

func (db *Db) recoverSegmentIndex(seg *segment) error {
	file, err := os.Open(seg.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	in := bufio.NewReader(file)
	var offset int64 = 0

	for {
		var record entry
		n, err := record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		seg.index[record.key] = offset
		db.mu.Lock()
		db.index[record.key] = segmentLocation{segID: seg.id, offset: offset}
		db.mu.Unlock()
		offset += int64(n)
	}
	return nil
}

func (db *Db) createNewSegment() error {
	var segPath string
	var id int

	if len(db.segments) == 0 {
		segPath = filepath.Join(db.dir, outFileName)
		id = 0
	} else {
		segPath = filepath.Join(db.dir, fmt.Sprintf("%s%d", segmentPrefix, db.nextSegID))
		id = db.nextSegID
		db.nextSegID++
	}

	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	seg := &segment{
		id:       id,
		file:     f,
		filePath: segPath,
		size:     0,
		index:    make(map[string]int64),
	}

	db.segments = append(db.segments, seg)
	db.out = seg

	return nil
}

func (db *Db) Close() error {
	var firstErr error
	db.closeOnce.Do(func() {
		close(db.writerDone)
		close(db.workerPool)

		db.writeMutex.Lock()
		defer db.writeMutex.Unlock()

		for _, seg := range db.segments {
			if seg.file != nil {
				if err := seg.file.Close(); err != nil && firstErr == nil {
					firstErr = fmt.Errorf("failed to close segment %s: %w", seg.filePath, err)
				}
			}
		}

		db.segments = nil
		db.out = nil
	})

	return firstErr
}

func (db *Db) Get(key string) (string, error) {
	db.mu.RLock()
	loc, ok := db.index[key]
	db.mu.RUnlock()

	if !ok {
		return "", ErrNotFound
	}

	db.mu.RLock()
	var seg *segment
	for _, s := range db.segments {
		if s.id == loc.segID {
			seg = s
			break
		}
	}
	db.mu.RUnlock()

	if seg == nil {
		return "", fmt.Errorf("segment %d not found", loc.segID)
	}

	resultChan := make(chan workerResponse, 1)
	db.workerPool <- workerRequest{
		key:      key,
		segID:    loc.segID,
		offset:   loc.offset,
		filePath: seg.filePath,
		result:   resultChan,
	}

	resp := <-resultChan
	return resp.value, resp.err
}

func (db *Db) doPut(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	data := e.Encode()

	if db.out.size+int64(len(data)) > db.maxSize {
		if err := db.createNewSegment(); err != nil {
			return err
		}
	}

	n, err := db.out.file.Write(data)
	if err != nil {
		return err
	}

	db.mu.Lock()
	db.out.index[key] = db.out.size
	db.index[key] = segmentLocation{segID: db.out.id, offset: db.out.size}
	db.mu.Unlock()

	db.out.size += int64(n)

	if len(db.segments) > 1 {
		go db.mergeSegments()
	}

	return nil
}

func (db *Db) Put(key, value string) error {
	errChan := make(chan error, 1)
	db.writeChan <- writeRequest{
		key:   key,
		value: value,
		err:   errChan,
	}
	return <-errChan
}

func (db *Db) mergeSegments() {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	if len(db.segments) <= 1 {
		return
	}

	tempPath := filepath.Join(db.dir, "merge-temp")
	tempFile, err := os.OpenFile(tempPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return
	}
	defer os.Remove(tempPath)
	defer tempFile.Close()

	newIndex := make(map[string]segmentLocation)
	var offset int64 = 0

	for i := len(db.segments) - 1; i >= 0; i-- {
		seg := db.segments[i]
		file, err := os.Open(seg.filePath)
		if err != nil {
			continue
		}

		reader := bufio.NewReader(file)
		var segOffset int64 = 0

		for {
			var record entry
			n, err := record.DecodeFromReader(reader)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				file.Close()
				continue
			}

			if _, exists := newIndex[record.key]; !exists {
				data := record.Encode()
				if _, err := tempFile.Write(data); err != nil {
					file.Close()
					continue
				}

				newIndex[record.key] = segmentLocation{segID: db.nextSegID, offset: offset}
				offset += int64(len(data))
			}

			segOffset += int64(n)
		}
		file.Close()
	}

	for _, seg := range db.segments {
		seg.file.Close()
	}

	newSegPath := filepath.Join(db.dir, fmt.Sprintf("%s%d", segmentPrefix, db.nextSegID))
	if err := os.Rename(tempPath, newSegPath); err != nil {
		db.recover()
		return
	}

	newSegFile, err := os.OpenFile(newSegPath, os.O_RDWR, 0600)
	if err != nil {
		db.recover()
		return
	}

	newSeg := &segment{
		id:       db.nextSegID,
		file:     newSegFile,
		filePath: newSegPath,
		size:     offset,
		index:    make(map[string]int64),
	}

	for k, loc := range newIndex {
		newSeg.index[k] = loc.offset
	}

	db.mu.Lock()
	db.segments = []*segment{newSeg}
	db.out = newSeg
	db.nextSegID++
	db.index = newIndex
	db.mu.Unlock()

	for _, seg := range db.segments[:len(db.segments)-1] {
		os.Remove(seg.filePath)
	}
}

func (db *Db) Size() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var total int64
	for _, seg := range db.segments {
		total += seg.size
	}
	return total, nil
}
