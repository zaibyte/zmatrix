package neo

import (
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zaipkg/xmath/xrand"

	"github.com/openacid/slim/index"
)

func TestIdxItemsSort(t *testing.T) {

	cnt := 1024
	items := make([]index.OffsetIndexItem, cnt)

	keyBuf := make([]byte, 8)

	xrand.Seed(time.Now().UnixNano())

	for i := range items {
		binary.LittleEndian.PutUint64(keyBuf, xrand.Uint64())
		items[i] = index.OffsetIndexItem{
			Key:    string(keyBuf),
			Offset: int64(i),
		}
	}

	sort.Sort(idxItems(items))

	n := cnt
	for i := n - 1; i > 0; i-- {
		if items[i].Key < items[i-1].Key {
			t.Fatal("not in asc order")
		}
	}
}

func TestMakeSegIdxHeader(t *testing.T) {

	buf := make([]byte, segIdxHeaderSize)
	min, max := make([]byte, 8), make([]byte, 8)

	xrand.Seed(time.Now().UnixNano())

	for i := 0; i < 128; i++ {
		minN := xrand.Uint64()
		binary.BigEndian.PutUint64(min, minN)
		binary.BigEndian.PutUint64(max, minN+1024)
		checksum := xrand.Uint64()
		idxSize := xrand.Uint64()
		makeSegIdxHeader(segIdxVersion1, checksum, idxSize, min, max, buf)

		version, checksumAct, idxSizeAct, minAct, maxAct, err := parseSegIdxHeader(buf)
		assert.Nil(t, err)
		assert.Equal(t, min, minAct)
		assert.Equal(t, max, maxAct)
		assert.Equal(t, checksum, checksumAct)
		assert.Equal(t, idxSize, idxSizeAct)
		assert.Equal(t, segIdxVersion1, version)
	}
}
