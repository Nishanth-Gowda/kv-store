package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const segmentPrefix = "wal-segment-"

// Extract the highest segment ID from a list of segment file paths
func GetLastSegmentID(files []string) (int, error) {
	if len(files) == 0 {
		return 0, nil
	}

	maxID := -1
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

		if id > maxID {
			maxID = id
		}
	}

	if maxID == -1 {
		return 0, nil
	}

	return maxID, nil
}

// Create a new log segment file with the given segment ID
func CreateLogSegmentFile(directory string, segmentID int) (*os.File, error) {
	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, segmentID))
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log segment file: %w", err)
	}
	return file, nil
}

// Sort segment files by their segment ID in ascending order
func SortSegmentFiles(files []string) ([]string, error) {
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
