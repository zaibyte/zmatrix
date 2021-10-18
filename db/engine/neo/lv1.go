package neo

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zaipkg/xstrconv"

	"github.com/cespare/xxhash/v2"
	"github.com/openacid/slim/index"
	"github.com/templexxx/bsegtree"
)

// Lvl1 Layout on local filesystem:
//  .
//  ├── <database_path>
//  │    ├── lv0
//  │    └── lv1
//  │         └── segments
//  │              ├── <segment_id>.seg
//  │              ├── <segment_id>.idx
//	│ 			   ├── ...

type lv1 struct {
	sync.RWMutex
	nextSegID uint32
	indexes   []*index.SlimIndex

	path string // <database_path>/lv1/

	sched xio.Scheduler
	fs    vfs.FS

	ranges bsegtree.Tree
}

const (
	segBufSize = 32 * 1024

	version1           = uint32(1) // In present, we only has one version.
	chunkIdxHeaderSize = 4 * 1024
)

// createPaths creates paths needed by lv1.
func (l *lv1) createPaths() error {

}

func (l *lv1) makeSeg(iter *pebble.Iterator) error {

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()
	}
	err = iter.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func (l *lv1) makeSegPath(id uint32) (seg, idx string) {

}

var candidatesPool = sync.Pool{New: func() interface{} {
	return make([]int, 0, 128) // Actually it will only use 1 if all chunks are sorted.
}}

// chunkCandidates returns candidates of chunks for a specific key.
func (l *lv1) chunkCandidates(key []byte) []int {
	l.RLock()
	defer l.RUnlock()

	keys := xstrconv.ToString(key)

	cnt := len(l.indexes)

	minC := sort.Search(cnt, func(i int) bool {
		start := l.indexes[i].start
		end := l.indexes[i].end

		if keys >= start && keys <= end {
			return true
		}
		return false
	})
	if minC == cnt {
		return nil
	}

	c := candidatesPool.Get().([]int)[:0]
	c = append(c, minC)

	for i := minC + 1; i < cnt; i++ {
		start := l.indexes[i].start
		end := l.indexes[i].end

		if keys >= start && keys <= end {
			c = append(c, i)
			continue
		}
		break
	}

	return c
}

func (l *lv1) addChunkIdx(items []index.OffsetIndexItem, logicTimestamp int64) error {

	idx, err := l.makeChunkIdx(items, logicTimestamp)
	if err != nil {
		return err
	}

	start, end := items[0].Key, items[len(items)-1].Key

	l.Lock()
	defer l.Unlock()

	l.indexes = append(l.indexes, rangeIdx{start: start, end: end, idx: idx})
	sort.Sort(rangeIdxes(l.indexes))

	l.fs.Rename()
}

// makeChunkIdx makes index of the chunk and persist it.
// logicTimestamp must be same as the chunk data's.
//
// chunk index on local:
// 4KB header + index_bytes
func (l *lv1) makeChunkIdx(items []index.OffsetIndexItem, logicTimestamp int64) (*index.SlimIndex, error) {
	idx, err := index.NewSlimIndex(items, nil)
	if err != nil {
		return nil, err
	}

	start, end := items[0].Key, items[len(items)-1].Key
	idxFn := filepath.Join(l.path, segsDir, tmpDir,
		fmt.Sprintf("%s.idx", strings.Join([]string{start, end, strconv.FormatInt(logicTimestamp, 10)}, "_")))

	f, err := l.fs.Create(idxFn)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	idxBytes, err := idx.Marshal()
	if err != nil {
		return nil, err
	}

	buf := xbytes.GetAlignedBytes(segBufSize)
	defer xbytes.PutBytes(buf)

	makeChunkIdxHeader(version1, xxhash.Sum64(idxBytes), uint64(len(idxBytes)), buf)
	undone := copy(buf[chunkIdxHeaderSize:], idxBytes)
	done, off := 0, int64(0)
	for done < len(idxBytes) {

		err = l.sched.DoSync(xio.ReqChunkWrite, f, off, buf)
		if err != nil {
			return nil, err
		}

		done += undone

		undone = copy(buf, idxBytes[done:])
		off += segBufSize
	}

	return idx, nil
}

func makeChunkIdxHeader(version uint32, checksum uint64, idxSize uint64, min, max, buf []byte) {

	binary.LittleEndian.PutUint32(buf, version)
	binary.LittleEndian.PutUint64(buf[4:], checksum)
	binary.LittleEndian.PutUint64(buf[12:], idxSize)
	binary.LittleEndian.PutUint32(buf[20:], uint32(len(min))) // longest key size is 255 bytes.
	copy(buf[24:], min)
	nextOff := 24 + len(min)
	binary.LittleEndian.PutUint32(buf[nextOff:], uint32(len(max)))
	nextOff += 4
	copy(buf[nextOff:], max)

	binary.LittleEndian.PutUint32(buf[chunkIdxHeaderSize-4:], 0)

	headerChecksum := xdigest.Sum32(buf)
	binary.LittleEndian.PutUint32(buf[chunkIdxHeaderSize-4:], headerChecksum)
}

func parseChunkIdxHeader(buf []byte) (version uint32, checksum uint64, idxSize uint64, err error) {

	c := binary.LittleEndian.Uint32(buf[chunkIdxHeaderSize-4:])

	binary.LittleEndian.PutUint32(buf[chunkIdxHeaderSize-4:], 0)
	headerChecksum := xdigest.Sum32(buf)
	if c != headerChecksum {
		return 0, 0, 0, xerrors.WithMessage(orpc.ErrChecksumMismatch, "failed to parse chunk index header")
	}

	version = binary.LittleEndian.Uint32(buf[:4])
	checksum = binary.LittleEndian.Uint64(buf[4:12])
	idxSize = binary.LittleEndian.Uint64(buf[12:18])
	return
}
