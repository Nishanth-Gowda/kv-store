package wal

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nishanth-gowda/kv-store/utils"
)

const (
	segmentPrefix = "wal-segment-"
)

type WAL struct {
	directory          string
	currentSegment     *os.File
	lock               sync.Mutex
	lastSequenceNumber uint64
	bufferedWriter     *bufio.Writer
	syncTimer          *time.Time
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
		lastSegmentId, err := utils.GetLastSegmentID(files)
		if err != nil {
			return nil, err
		}
	} else {
		// create the new log segment
		file, err := utils.CreateLogSegmentFile(directory, 0)
		if err != nil {
			return nil, err
		}
	}
}
