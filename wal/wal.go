package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nishanth-gowda/kv-store/utils"
)

const (
	syncInterval  = 100 * time.Millisecond
	segmentPrefix = "wal-segment-"
)

type WAL struct {
	directory          string
	currentSegment     *os.File
	lock               sync.Mutex
	lastSequenceNumber uint64
	bufferedWriter     *bufio.Writer
	syncTimer          *time.Timer
	forceFSync         bool
	maxFileSize        int
	maxSegments        int
	ctx                context.Context
	cancel             context.CancelFunc
}

func NewWal(directory string, forceSync bool, maxFileSize int, maxSegments int) (*WAL, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentId int
	if len(files) > 0 {
		// find the last segmentId
		lastSegmentId, err = utils.GetLastSegmentID(files)
		if err != nil {
			return nil, err
		}
	} else {
		// create the new log segment
		file, err := utils.CreateLogSegmentFile(directory, 0)
		if err != nil {
			return nil, err
		}

		if err := file.Close(); err != nil {
			return nil, err
		}
	}

	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentId))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// seek to the end of the file
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wal := &WAL{
		directory:          directory,
		currentSegment:     file,
		lastSequenceNumber: 0,
		bufferedWriter:     bufio.NewWriter(file),
		syncTimer:          time.NewTimer(syncInterval),
		forceFSync:         forceSync,
		maxFileSize:        maxFileSize,
		maxSegments:        maxSegments,
		ctx:                ctx,
		cancel:             cancel,
	}

	if wal.lastSequenceNumber, err = utils.GetLastSequenceNumber(filePath); err != nil {
		return nil, err
	}

	go wal.syncLoop()

	return wal, nil

}

func (wal *WAL) getLastSequenceNumber() (uint64, error) {

	entry, err := wal.getLastEntryInLog()
	if err != nil {
		return 0, err
	}

	if entry != nil {
		return entry.GetLogSequenceNumber(), nil
	}

	return entry.SequenceNumber, nil
}

func (wal *WAL) getLastEntryInLog() (*WAL_Entry, error) {

	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	var previousSize int32
	var offset int64
	var entry *WAL_Entry

	for {
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {

				// we have reached the end of the file, read the last entry
				// at the saved offset and return it
				if offset == 0 {
					return entry, nil
				}

				// seek to the beginning of the file
				if _, err := file.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}

				data := make([]byte, previousSize)
				if _, err := io.ReadFull(file, data); err != nil {
					return nil, err
				}

				entry, err := unMarshalAndVerifyEntry(data)
				if err != nil {
					return nil, err
				}

				return entry, nil
			}
			
			return nil, err
		}
		
		// Get current offset
		offset, err = file.Seek(0, io.SeekCurrent)
		previousSize = size
		
		if err != nil {
			return nil, err
		}
		
		if _, err := file.Seek(int64(size), io.SeekCurrent); err != nil {
			return nil, err
		}
	}
}
