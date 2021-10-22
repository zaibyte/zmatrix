package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	"g.tesamc.com/IT/zaipkg/xstrconv"
)

type bytess [][]byte

func (e bytess) Len() int {
	return len(e)
}

func (e bytess) Less(i, j int) bool {

	return bytes.Compare(e[i], e[j]) == -1
}

func (e bytess) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func BenchmarkSortBillion(b *testing.B) {

	bs := make([]string, 4194304)
	for i := range bs {
		bb := make([]byte, 8)
		binary.LittleEndian.PutUint64(bb, uint64(i))
		bs[i] = xstrconv.ToString(bb)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sort.Strings(bs)
	}
}

func TestStrBytesCompare(t *testing.T) {

	a := "ba"
	b := "b"

	fmt.Println(a > b)
	fmt.Println(bytes.Compare([]byte(a), []byte(b)))
}
