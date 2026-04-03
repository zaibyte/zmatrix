package neo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/zaibyte/zaipkg/xtest"

	"github.com/openacid/low/size"

	"github.com/zaibyte/zaipkg/directio"
	"github.com/zaibyte/zaipkg/orpc"
	"github.com/zaibyte/zaipkg/xio"
	"github.com/zaibyte/zaipkg/xlog"

	_ "github.com/zaibyte/zaipkg/xlog/xlogtest"
	"github.com/zaibyte/zaipkg/xmath/xrand"
	"github.com/zaibyte/zmatrix/pkg/config"

	"github.com/cockroachdb/pebble"
	"github.com/openacid/slim/index"
	"github.com/stretchr/testify/assert"
)

func TestIdxMemoryUsage(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("prop testing is not enabled")
	}

	t.Logf("4194304 kv will take %.2f bits for each key as index", idxMem(4194304))
}

func idxMem(keyCnt int) float64 {

	items := make([]index.OffsetIndexItem, keyCnt)

	key := make([]byte, 8)
	off := int64(0)
	for i := range items {

		if i > 0 && i%20 == 0 {
			off += blockGainSize
		}

		binary.BigEndian.PutUint64(key, uint64(i))
		items[i].Key = string(key)
		items[i].Offset = off
	}

	idx, err := index.NewSlimIndex(items, nil)
	if err != nil {
		panic(err)
	}

	sz := size.Of(idx)

	return float64(sz) * 8 / float64(keyCnt)
}

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

	minN := xrand.Uint64()
	binary.BigEndian.PutUint64(min, minN)
	binary.BigEndian.PutUint64(max, minN+1024)
	checksum := xrand.Uint64()
	idxSize := xrand.Uint64()
	makeSegIdxHeader(segIdxVersion1, checksum, idxSize, min, max, buf)

	buf[0] += 1

	_, _, _, _, _, err := parseSegIdxHeader(buf)
	assert.True(t, errors.Is(err, orpc.ErrChecksumMismatch))
}

// This testing is built for ensuring thread-safe and could return the right segment.
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
	defer l.close()

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

	// Search is faster than add, and it has more goroutines, which means searching needs waiting sometimes.
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

// Make just one segment, and searching it.
// It's enough because we've done multi segment searching in TestLv1AddSearchSegRange.
func TestLv1MakeSearchSeg(t *testing.T) {
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
	defer l.close()

	dir, err := ioutil.TempDir(os.TempDir(), "pebble")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	l0, err := pebble.Open(dir, &pebble.Options{
		Logger: xlog.GetGRPCLoggerV2(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l0.Close()

	cnt := 128
	kLen := 8 // fixed key length helping to accelerate testing, and the lengths of values are enough random.
	keyBuf := make([]byte, kLen)
	valBuf := make([]byte, config.MaxValueLen)

	rand.Seed(time.Now().UnixNano())
	rand.Read(valBuf) // We don't need too many value.

	minSize := int64(0)

	for i := 0; i < cnt-5; i++ {

		binary.BigEndian.PutUint64(keyBuf, uint64(1024-i))

		// vLen := xrand.Uint32n(uint32(config.MaxValueLen))
		vLen := xrand.Uint32n(uint32(config.MaxValueLen / 4)) // Avoiding too slow testing.
		if vLen == 0 {
			vLen = 1
		}

		err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
		if err != nil {
			t.Fatal(err)
		}
		minSize += int64(vLen)
	}

	// Ensure there are < KB, == KB, > blockGainSize, B, 4MB value.
	vLen := blockGainSize + 1025
	binary.BigEndian.PutUint64(keyBuf, uint64(5))
	err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	minSize += int64(vLen)
	vLen = 1022
	binary.BigEndian.PutUint64(keyBuf, uint64(4))
	err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	minSize += int64(vLen)
	vLen = 1024
	binary.BigEndian.PutUint64(keyBuf, uint64(3))
	err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	minSize += int64(vLen)
	vLen = 1
	binary.BigEndian.PutUint64(keyBuf, uint64(2))
	err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	minSize += int64(vLen)
	vLen = 4 * 1024 * 1024
	binary.BigEndian.PutUint64(keyBuf, uint64(1))
	err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	minSize += int64(vLen)

	snap := l0.NewSnapshot()

	id, idx, min, max, added, err := l.makeSegIdx(snap, 1024, minSize)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int64(0), id)
	assert.Equal(t, cnt, added)
	err = l.persistIdx(id, idx, min, max)
	if err != nil {
		t.Fatal(err)
	}
	l.addSegIdxRange(id, idx, min, max)

	iter := snap.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()

		v, closer, err := l.search(k)
		if err != nil {
			// t.Fatalf("failed to search for %d: %s", binary.BigEndian.Uint64(k), err.Error())
			t.Logf("failed to search for %d: %s", binary.BigEndian.Uint64(k), err.Error())
		}
		if err == nil {

			if len(v) != len(iter.Value()) {
				t.Fatal("value length mismatched")
			}

			if !bytes.Equal(v, iter.Value()) {
				t.Fatal("value mismatched")
			}
			_ = closer.Close()
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(4)

	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			iter := snap.NewIter(nil)
			defer iter.Close()

			for iter.First(); iter.Valid(); iter.Next() {
				k := iter.Key()

				v, closer, err := l.search(k)
				if err != nil {
					t.Errorf("failed to search: %s", err.Error())
					return
				}
				if err == nil {
					if !bytes.Equal(v, iter.Value()) {
						t.Error("value mismatched")
						return
					}
					_ = closer.Close()
				}
			}
		}()
	}
	wg.Wait()
}

func TestLv1PersistIdx(t *testing.T) {

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
	defer l.close()

	cnt := 1024
	items := make([]index.OffsetIndexItem, cnt)
	keyBuf := make([]byte, 8)
	for i := range items {

		binary.BigEndian.PutUint64(keyBuf, uint64(i))

		items[i] = index.OffsetIndexItem{
			Key:    string(keyBuf),
			Offset: 0,
		}
	}
	idx, err := index.NewSlimIndex(items, nil)
	if err != nil {
		t.Fatal(err)
	}

	min, max := make([]byte, 8), make([]byte, 8)
	binary.BigEndian.PutUint64(min, 0)
	binary.BigEndian.PutUint64(max, 1023)
	err = l.persistIdx(0, idx, min, max)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, segIdxLoadBufSize)

	actIdx, actMin, actMax, err := loadSegIdxFromFile(l.fs, filepath.Join(l.segsPath, "0.idx"), buf)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, min, actMin)
	assert.Equal(t, max, actMax)

	idxB, err := idx.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	actIdxB, err := actIdx.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, idxB, actIdxB)

	idxF, err := l.fs.Open(filepath.Join(l.segsPath, "0.idx"))
	if err != nil {
		t.Fatal(err)
	}
	tb := directio.AlignedBlock(4096)
	_, err = idxF.ReadAt(tb, 4096)
	if err != nil {
		t.Fatal(err)
	}
	tb[0] += 1
	_, err = idxF.WriteAt(tb, 4096)
	if err != nil {
		t.Fatal(err)
	}

	_, _, _, err = loadSegIdxFromFile(l.fs, filepath.Join(l.segsPath, "0.idx"), buf)
	assert.True(t, errors.Is(err, orpc.ErrChecksumMismatch))
}

func TestLv1Load(t *testing.T) {
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
	defer l.close()

	dir, err := ioutil.TempDir(os.TempDir(), "pebble")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	l0, err := pebble.Open(dir, &pebble.Options{
		Logger: xlog.GetGRPCLoggerV2(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l0.Close()

	cnt := 128
	kLen := 8 // fixed key length helping to accelerate testing, and the lengths of values are enough random.
	keyBuf := make([]byte, kLen)
	valBuf := make([]byte, config.MaxValueLen)

	rand.Seed(time.Now().UnixNano())
	rand.Read(valBuf) // We don't need too many value.

	minSize := int64(0)

	for i := 0; i < cnt-5; i++ {

		binary.BigEndian.PutUint64(keyBuf, uint64(1024-i))

		// vLen := xrand.Uint32n(uint32(config.MaxValueLen))
		vLen := xrand.Uint32n(uint32(config.MaxValueLen / 4)) // Avoiding too slow testing.

		err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
		if err != nil {
			t.Fatal(err)
		}
		minSize += int64(vLen)
	}

	// Ensure there are < KB, == KB, > blockGainSize, B, 4MB value.
	vLen := blockGainSize + 1025
	binary.BigEndian.PutUint64(keyBuf, uint64(5))
	err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	minSize += int64(vLen)
	vLen = 1022
	binary.BigEndian.PutUint64(keyBuf, uint64(4))
	err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	minSize += int64(vLen)
	vLen = 1024
	binary.BigEndian.PutUint64(keyBuf, uint64(3))
	err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	minSize += int64(vLen)
	vLen = 1
	binary.BigEndian.PutUint64(keyBuf, uint64(2))
	err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	minSize += int64(vLen)
	vLen = 4 * 1024 * 1024
	binary.BigEndian.PutUint64(keyBuf, uint64(1))
	err = l0.Set(keyBuf[:kLen], valBuf[:vLen], pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	minSize += int64(vLen)

	snap := l0.NewSnapshot()

	id, idx, min, max, added, err := l.makeSegIdx(snap, 1024, minSize)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int64(0), id)
	assert.Equal(t, cnt, added)
	err = l.persistIdx(id, idx, min, max)
	if err != nil {
		t.Fatal(err)
	}
	l.addSegIdxRange(id, idx, min, max)

	err = l.persistIdx(id, idx, min, max)
	if err != nil {
		t.Fatal(err)
	}
	ll, err := loadLv1(dbPath, fs, &xio.NopScheduler{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	err = ll.load()
	if err != nil {
		t.Fatal(err)
	}

	iter := snap.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()

		v, closer, err := ll.search(k)
		if err != nil {
			t.Fatalf("failed to search: %s", err.Error())
		}
		if err == nil {
			if !bytes.Equal(v, iter.Value()) {
				t.Fatal("value mismatched")
			}
			_ = closer.Close()
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(4)

	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			iter := snap.NewIter(nil)
			defer iter.Close()

			for iter.First(); iter.Valid(); iter.Next() {
				k := iter.Key()

				v, closer, err := ll.search(k)
				if err != nil {
					t.Errorf("failed to search: %s", err.Error())
					return
				}
				if err == nil {
					if !bytes.Equal(v, iter.Value()) {
						t.Error("value mismatched")
						return
					}
					_ = closer.Close()
				}
			}
		}()
	}
	wg.Wait()
}

func TestSearchSeg(t *testing.T) {

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
	defer l.close()

	min, max := make([]byte, 8), make([]byte, 8)
	begin := uint64(0)
	for i := 0; i < 28; i++ {
		binary.BigEndian.PutUint64(min, begin)
		begin += 4196090
		binary.BigEndian.PutUint64(max, begin)
		l.addRange(min, max)
		begin += 1
	}

	key := make([]byte, 8)
	mid := uint64(4196090) / 2
	for i := 0; i < 28; i++ {
		binary.BigEndian.PutUint64(key, mid)
		ids, ok := l.searchSeg(key)
		if !ok {
			t.Fatal("seg must be searched")
		}
		if len(ids) != 1 {
			t.Fatal("total seg count must be 1")
		}
		if ids[0] != i {
			t.Fatalf("seg mismatched, exp: %d, got: %d", i, ids[0])
		}
		mid += 4196090
	}
}
