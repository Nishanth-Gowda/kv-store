package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func GetLastSegmentID(files []string) (int, error) {
	var lastSegmentId int
	for _, file := range files {
		_, fileName := filepath.Split(file)
		segment_id, err := strconv.Atoi(strings.TrimPrefix(fileName, "wal-segment-"))
		if err != nil {
			return 0, err
		}

		if segment_id > lastSegmentId {
			lastSegmentId = segment_id
		}
	}
	return lastSegmentId, nil
}

func CreateLogSegmentFile(directory string, segmentId int) (*os.File, error) {
	filePath := filepath.Join(directory, fmt.Sprintf("wal-segment-%d", segmentId))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	
	return file, nil
}

