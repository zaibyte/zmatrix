package neo

import (
	"encoding/binary"
	"sort"
	"testing"
	"time"

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

}
