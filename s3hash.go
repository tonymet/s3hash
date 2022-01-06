package s3hash

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
)

const IOBUF = 1024 * 1024 * 50
const BUFSIZE = int64(1024 * 1024 * 10)

// CalculateForFile calculates the S3 hash of a given file with the given chunk size
func CalculateForFile(filename string, chunkSize int64) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()
	stat, err := os.Stat(filename)
	if err != nil {
		panic(err)
	}

	return Calculate(f, chunkSize, stat.Size())
}

// Calculate calculates the S3 hash of a given io.ReadSeeker with the given chunk size.
func Calculate(f io.Reader, chunkSize int64, dataSize int64) (string, error) {
	chunks := dataSize / chunkSize

	var (
		sumOfSums []byte = make([]byte, 0, dataSize/chunkSize)
		parts     int
	)
	for i := int64(0); i < dataSize; {
		lenRead, sum, err := md5sum(&f, chunkSize)
		fmt.Printf("lenRead: %d\n", lenRead)
		if err != nil {
			return "", err
		}
		sumOfSums = append(sumOfSums, sum...)
		parts++
		fmt.Printf("\r Complete  %d / %d chunks,  %0.2f %%", i/chunkSize, chunks, float64(i)/float64(dataSize)*100)
		i += int64(lenRead)
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

func md5sum(r *io.Reader, length int64) (int, []byte, error) {
	var bufSize = BUFSIZE
	if length < bufSize {
		bufSize = length
	}
	var (
		buf              = make([]byte, bufSize)
		h                = md5.New()
		bufRead, lenRead int
		err              error
	)
	for ; length > 0; length -= int64(bufRead) {
		bufRead, err = (*r).Read(buf)
		h.Write(buf[:bufRead])
		lenRead += bufRead
		if err != nil {
			//panic(err)
			return lenRead, h.Sum(nil), nil
		}
		// truncate to lenRead
	}
	return lenRead, h.Sum(nil), nil
}
