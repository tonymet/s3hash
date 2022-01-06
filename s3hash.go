package s3hash

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
)

type NotifyEvent struct {
	i                   int64
	chunkSize, dataSize int64
	parts               int
}

func (e NotifyEvent) progress() float64 {
	return float64(e.i) / float64(e.dataSize) * 100
}

type NotifyCallback = func(e NotifyEvent)

func printProgress(e NotifyEvent) {
	fmt.Printf("\r Complete  %d / %d chunks,  %0.2f %%", e.parts, e.chunks(), e.progress())
}

// CalculateForFile calculates the S3 hash of a given file with the given chunk size
func CalculateForFile(filename string, chunkSize int64) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()

	return Calculate(f, chunkSize, printProgress)
}

func (e NotifyEvent) chunks() (chunks int64) {
	chunks = e.dataSize / e.chunkSize
	if e.dataSize%e.chunkSize != 0 {
		chunks++
	}
	return
}

// Calculate calculates the S3 hash of a given io.ReadSeeker with the given chunk size.
func Calculate(f io.ReadSeeker, chunkSize int64, progress NotifyCallback) (string, error) {
	dataSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return "", err
	}

	var (
		sumOfSums []byte
		parts     int
	)
	for i := int64(0); i < dataSize; i += chunkSize {
		length := chunkSize
		if i+chunkSize > dataSize {
			length = dataSize - i
		}
		sum, err := md5sum(f, i, length)
		if err != nil {
			return "", err
		}
		sumOfSums = append(sumOfSums, sum...)
		parts++
		progress(NotifyEvent{i, chunkSize, dataSize, parts})
	}

	var finalSum []byte

	if parts == 1 {
		finalSum = sumOfSums
	} else {
		h := md5.New()
		_, err := h.Write(sumOfSums)
		if err != nil {
			return "", err
		}
		finalSum = h.Sum(nil)
	}

	sumHex := hex.EncodeToString(finalSum)

	if parts > 1 {
		sumHex += "-" + strconv.Itoa(parts)
	}

	return sumHex, nil
}

func md5sum(r io.ReadSeeker, start, length int64) ([]byte, error) {
	r.Seek(start, io.SeekStart)
	h := md5.New()
	if _, err := io.CopyN(h, r, length); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}
