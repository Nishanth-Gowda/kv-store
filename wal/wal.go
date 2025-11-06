package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nishanth-gowda/kv-store/utils"
)

const (
	syncInterval  = 100 * time.Millisecond
	segmentPrefix = "wal-segment-"
)

// EntryType represents the type of WAL entry
type EntryType uint8

const (
	EntryTypeSET    EntryType = 1
	EntryTypeDELETE EntryType = 2
)

// WAL_Entry represents a single entry in the WAL
type WAL_Entry struct {
	Type              EntryType
	SequenceNumber    uint64
	Key               string
	Value             []byte
	ExpiresAtUnixNano int64 // 0 means no expiration
	CRC               uint32
}

// GetLogSequenceNumber returns the sequence number of the entry
func (e *WAL_Entry) GetLogSequenceNumber() uint64 {
	return e.SequenceNumber
}

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

	if wal.lastSequenceNumber, err = getLastSequenceNumberFromFile(filePath); err != nil {
		return nil, err
	}

	go wal.syncLoop()

	return wal, nil

}

// Marshal serializes a WAL_Entry to bytes
func Marshal(entry *WAL_Entry) ([]byte, error) {
	// Calculate CRC before marshaling
	entry.CRC = calculateCRC(entry)

	var buf []byte
	encoder := gob.NewEncoder(&buffer{data: &buf})
	if err := encoder.Encode(entry); err != nil {
		return nil, err
	}
	return buf, nil
}

// MustUnmarshal deserializes bytes to a WAL_Entry (panics on error)
func MustUnmarshal(data []byte, entry *WAL_Entry) {
	decoder := gob.NewDecoder(&buffer{data: &data})
	if err := decoder.Decode(entry); err != nil {
		panic(fmt.Sprintf("failed to unmarshal WAL entry: %v", err))
	}
}

// calculateCRC calculates CRC32 checksum for the entry (excluding CRC field)
func calculateCRC(entry *WAL_Entry) uint32 {
	// Create a copy without CRC for checksum calculation
	tempEntry := *entry
	tempEntry.CRC = 0

	var buf []byte
	encoder := gob.NewEncoder(&buffer{data: &buf})
	if err := encoder.Encode(&tempEntry); err != nil {
		return 0
	}
	return crc32.ChecksumIEEE(buf)
}

// verifyCRC verifies the CRC checksum of an entry
func verifyCRC(entry *WAL_Entry) bool {
	expectedCRC := calculateCRC(entry)
	return entry.CRC == expectedCRC
}

// buffer is a simple buffer implementation for gob encoder/decoder
type buffer struct {
	data *[]byte
	pos  int
}

func (b *buffer) Write(p []byte) (n int, err error) {
	*b.data = append(*b.data, p...)
	return len(p), nil
}

func (b *buffer) Read(p []byte) (n int, err error) {
	if b.pos >= len(*b.data) {
		return 0, io.EOF
	}
	n = copy(p, (*b.data)[b.pos:])
	b.pos += n
	return n, nil
}

func unMarshalAndVerifyEntry(data []byte) (*WAL_Entry, error) {
	var entry WAL_Entry
	MustUnmarshal(data, &entry)

	if !verifyCRC(&entry) {
		return nil, fmt.Errorf("invalid CRC")
	}
	return &entry, nil
}

// Append writes a new entry to the WAL
func (wal *WAL) Append(entryType EntryType, key string, value []byte, expiresAtUnixNano int64) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	// Increment sequence number
	wal.lastSequenceNumber++

	entry := &WAL_Entry{
		Type:              entryType,
		SequenceNumber:    wal.lastSequenceNumber,
		Key:               key,
		Value:             value,
		ExpiresAtUnixNano: expiresAtUnixNano,
	}

	// Marshal entry
	data, err := Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	// Check if we need to rotate segment
	if err := wal.checkAndRotateSegment(); err != nil {
		return fmt.Errorf("failed to rotate segment: %w", err)
	}

	// Write size prefix (int32)
	size := int32(len(data))
	if err := binary.Write(wal.bufferedWriter, binary.LittleEndian, size); err != nil {
		return fmt.Errorf("failed to write size: %w", err)
	}

	// Write entry data
	if _, err := wal.bufferedWriter.Write(data); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	// Flush buffer
	if err := wal.bufferedWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	// Force fsync if configured
	if wal.forceFSync {
		if err := wal.currentSegment.Sync(); err != nil {
			return fmt.Errorf("failed to sync: %w", err)
		}
	}

	return nil
}

// checkAndRotateSegment checks if segment rotation is needed and performs it
func (wal *WAL) checkAndRotateSegment() error {
	// Get current file size
	stat, err := wal.currentSegment.Stat()
	if err != nil {
		return err
	}

	if stat.Size() < int64(wal.maxFileSize) {
		return nil
	}

	// Close current segment
	if err := wal.bufferedWriter.Flush(); err != nil {
		return err
	}
	if err := wal.currentSegment.Sync(); err != nil {
		return err
	}
	if err := wal.currentSegment.Close(); err != nil {
		return err
	}

	// Find next segment ID
	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil {
		return err
	}

	nextSegmentID := 0
	if len(files) > 0 {
		lastSegmentID, err := utils.GetLastSegmentID(files)
		if err != nil {
			return err
		}
		nextSegmentID = lastSegmentID + 1
	}

	// Clean up old segments if needed
	if err := wal.cleanupOldSegments(files); err != nil {
		return err
	}

	// Create new segment
	filePath := filepath.Join(wal.directory, fmt.Sprintf("%s%d", segmentPrefix, nextSegmentID))
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	wal.currentSegment = file
	wal.bufferedWriter = bufio.NewWriter(file)

	return nil
}

// cleanupOldSegments removes old segments if we exceed maxSegments
func (wal *WAL) cleanupOldSegments(files []string) error {
	if len(files) < wal.maxSegments {
		return nil
	}

	// Sort files by segment ID and remove oldest ones
	sortedFiles, err := sortSegmentFiles(files)
	if err != nil {
		return err
	}

	// Remove oldest segments
	toRemove := len(sortedFiles) - wal.maxSegments + 1 // +1 because we're about to create a new one
	for i := 0; i < toRemove && i < len(sortedFiles); i++ {
		if err := os.Remove(sortedFiles[i]); err != nil {
			return fmt.Errorf("failed to remove old segment %s: %w", sortedFiles[i], err)
		}
	}

	return nil
}

// Sync the WAL to disk with predefined interval by using a timer
func (wal *WAL) syncLoop() {
	for {
		select {
		case <-wal.ctx.Done():
			return
		case <-wal.syncTimer.C:
			wal.lock.Lock()
			if wal.bufferedWriter != nil {
				// Flush the buffered writer to the current segment file
				if err := wal.bufferedWriter.Flush(); err != nil {
					fmt.Printf("Error flushing buffered writer: %v\n", err)
				}
			}
			if wal.currentSegment != nil {
				// Sync the current segment file to disk
				if err := wal.currentSegment.Sync(); err != nil {
					fmt.Printf("Error syncing current segment: %v\n", err)
				}
			}
			wal.lock.Unlock()

			// Reset timer
			wal.syncTimer.Reset(syncInterval)
		}
	}
}

// Closes the WAL and clean up resources
func (wal *WAL) Close() error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	// Cancel context to stop sync loop
	wal.cancel()

	// Stop timer
	if wal.syncTimer != nil {
		wal.syncTimer.Stop()
	}

	// Flush and close any buffered writer and current segment file
	if wal.bufferedWriter != nil {
		if err := wal.bufferedWriter.Flush(); err != nil {
			return err
		}
	}

	if wal.currentSegment != nil {
		if err := wal.currentSegment.Sync(); err != nil {
			return err
		}
		if err := wal.currentSegment.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Reads all entries from all WAL segments and returns them as a slice of WAL_Entry
func (wal *WAL) ReadAll() ([]*WAL_Entry, error) {
	var allEntries []*WAL_Entry

	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	sortedFiles, err := sortSegmentFiles(files)
	if err != nil {
		return nil, err
	}

	// Read entries from each segment
	for _, filePath := range sortedFiles {
		entries, err := wal.readSegment(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read segment %s: %w", filePath, err)
		}
		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}

// readSegment reads all entries from a single segment file
func (wal *WAL) readSegment(filePath string) ([]*WAL_Entry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []*WAL_Entry

	for {
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Read entry data
		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			if err == io.EOF {
				// Partial entry at end of file, skip it
				break
			}
			return nil, err
		}

		// Unmarshal and verify entry
		entry, err := unMarshalAndVerifyEntry(data)
		if err != nil {
			// Invalid entry, stop reading this segment
			break
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// getLastSequenceNumberFromFile reads the last sequence number from a segment file
func getLastSequenceNumberFromFile(filePath string) (uint64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer file.Close()

	var previousSize int32
	var offset int64

	for {
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				// We've reached the end, read the last entry
				if offset == 0 {
					return 0, nil // Empty file
				}

				// Seek to the beginning of the last entry
				if _, err := file.Seek(offset, io.SeekStart); err != nil {
					return 0, err
				}

				// Read the entry data
				data := make([]byte, previousSize)
				if _, err := io.ReadFull(file, data); err != nil {
					return 0, err
				}

				// Unmarshal entry to get sequence number
				var entry WAL_Entry
				MustUnmarshal(data, &entry)
				return entry.SequenceNumber, nil
			}
			return 0, err
		}

		// Save current offset before skipping
		offset, err = file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}

		previousSize = size

		// Skip the entry data
		if _, err := file.Seek(int64(size), io.SeekCurrent); err != nil {
			return 0, err
		}
	}
}

// sortSegmentFiles sorts segment files by their segment ID in ascending order
func sortSegmentFiles(files []string) ([]string, error) {
	type segmentInfo struct {
		path string
		id   int
	}

	segments := make([]segmentInfo, 0, len(files))
	for _, file := range files {
		baseName := filepath.Base(file)
		if !strings.HasPrefix(baseName, segmentPrefix) {
			continue
		}

		idStr := strings.TrimPrefix(baseName, segmentPrefix)
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue // Skip invalid segment names
		}

		segments = append(segments, segmentInfo{
			path: file,
			id:   id,
		})
	}

	// Sort by segment ID
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].id < segments[j].id
	})

	// Extract sorted paths
	sortedFiles := make([]string, len(segments))
	for i, seg := range segments {
		sortedFiles[i] = seg.path
	}

	return sortedFiles, nil
}
