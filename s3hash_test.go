package s3hash

import (
	"bytes"
	"os"
	"testing"
)

const bytesInMb = 1024 * 1024

type hashTest struct {
	out        string
	genesis    string
	numRepeats int
	chunkSize  int64
}

var golden = []hashTest{
	// Single-part run
	{"bf8043c1e6890929374ea8f19828acbb", "Time flies like an arrow; fruit flies like a banana", 1, bytesInMb},

	// Multipart run
	{"38a7e5991be21b577978abb001323b0a-20", "0123456789", 1e7, 5 * bytesInMb},
}

func TestGolden(t *testing.T) {
	for i, g := range golden {
		data := bytes.Repeat([]byte(g.genesis), g.numRepeats)
		rdr := bytes.NewReader(data)
		result, err := Calculate(rdr, g.chunkSize, int64(len(g.genesis)*g.numRepeats))
		if err != nil {
			t.Fatalf("Error calculating golden #%v: %v", i, err)
		}
		if result != g.out {
			t.Fatalf("hash[%d](%s)(%d) = %s want %s", i, g.genesis, g.numRepeats, result, g.out)
		}
	}
}

func TestFile(t *testing.T) {
	filename := "test/testfile"
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	stat, err := os.Stat(filename)
	if err != nil {
		panic(err)
	}
	result, err := Calculate(file, 5*bytesInMb, stat.Size())
	if err != nil {
		t.Fatalf("Error")
	}
	if result != "b1900dcc858c1fc72d2e798b946f7b54-2" {
		t.Fatalf("no match for file")
	}
}

// from fib_test.go
func BenchmarkGolden(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for i, g := range golden {
			data := bytes.Repeat([]byte(g.genesis), g.numRepeats)
			rdr := bytes.NewReader(data)
			result, err := Calculate(rdr, g.chunkSize, int64(len(g.genesis)*g.numRepeats))
			if err != nil {
				b.Fatalf("Error calculating golden #%v: %v", i, err)
			}
			if result != g.out {
				b.Fatalf("hash[%d](%s)(%d) = %s want %s", i, g.genesis, g.numRepeats, result, g.out)
			}
		}
	}
}


func BenchmarkFile(b *testing.B) {
	filename := "test/testfile"
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	stat, err := os.Stat(filename)
	if err != nil {
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		result, err := Calculate(file, 5*bytesInMb, stat.Size())
		if err != nil {
			b.Fatalf("Error")
		}
		if result != "b1900dcc858c1fc72d2e798b946f7b54-2" {
			b.Fatalf("no match for file")
		}
	}
}