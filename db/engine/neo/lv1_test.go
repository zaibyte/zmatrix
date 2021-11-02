package neo

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"g.tesamc.com/IT/zaipkg/xio"

	"g.tesamc.com/IT/zaipkg/xmath/xrand"

	"github.com/openacid/slim/index"
	"github.com/stretchr/testify/assert"
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

func TestLv1AddSearchSegRange(t *testing.T) {

	fs := testFS

	dbPath := filepath.Join(os.TempDir(), "neo.lv1", fmt.Sprintf("%d", xrand.Uint32()))

	err := fs.MkdirAll(dbPath, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll(dbPath)

	l, err := createLv1(dbPath, fs, &xio.NopScheduler{})
	if err != nil {
		t.Fatal(err)
	}

	cnt := maxSegs

	writeDone := make(chan struct{})

	go func() {
		defer func() {
			close(writeDone)
		}()
		min, max := make([]byte, 8), make([]byte, 8)
		start := 0
		for i := 0; i < cnt; i++ {
			binary.BigEndian.PutUint64(min, uint64(start))
			binary.BigEndian.PutUint64(max, uint64(start)+1024)
			l.addRange(min, max)
			start += 2048
		}
	}()

	wg2 := new(sync.WaitGroup)
	wg2.Add(4)

	for i := 0; i < 4; i++ {

		go func() {
			defer wg2.Done()

			key := make([]byte, 8)
			keyStart := 512
			for j := 0; j < cnt; j++ {
				binary.BigEndian.PutUint64(key, uint64(keyStart))

				select {
				case <-writeDone:
					ids, ok := l.searchSeg(key)
					assert.True(t, ok)
					assert.Equal(t, []int{j}, ids)
				default:
					for {
						ids, ok := l.searchSeg(key)
						if ok {
							assert.Equal(t, []int{j}, ids)
							break
						} else {
							time.Sleep(time.Microsecond)
						}
					}
				}
				keyStart += 2048

			}
		}()
	}

	wg2.Wait()
}
