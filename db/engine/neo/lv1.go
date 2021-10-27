package neo

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"g.tesamc.com/IT/zaipkg/directio"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zaipkg/xstrconv"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
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

const (
	segSuffix    = ".seg"
	segIdxSuffix = ".idx"
)

type lv1 struct {
	nextSegID int64            // start from 1.
	indexes   []unsafe.Pointer // *index.SlimIndex

	segsPath string
	segSize  int64

	sched xio.Scheduler
	fs    vfs.FS

	ranges unsafe.Pointer // *bsegtree.BSTree
}

const (
	segBufSize = 32 * 1024

	version1           = uint32(1) // In present, we only has one version.
	chunkIdxHeaderSize = 4 * 1024
)

func createLv1(dbPath string, fs vfs.FS, sched xio.Scheduler, segSize int64) (l *lv1, err error) {

	sp, err := createLv1Paths(dbPath, fs)
	if err != nil {
		return nil, err
	}

	t := new(bsegtree.BSTree)
	t.Clear()

	l = &lv1{
		nextSegID: 1,
		indexes:   make([]unsafe.Pointer, 1024, 1024),
		segsPath:  sp,
		segSize:   segSize,
		sched:     sched,
		fs:        fs,
		ranges:    unsafe.Pointer(t),
	}

	return
}

// createLv1Paths create paths needed by lv1, and return segments paths.
func createLv1Paths(dbPath string, fs vfs.FS) (string, error) {
	dir := makeL1Dir(dbPath)
	err := fs.MkdirAll(dir, 0755)
	if err != nil {
		return "", err
	}

	sp := makeL1SegDir(dir)
	err = fs.MkdirAll(sp, 0755)
	if err != nil {
		return "", err
	}
	return sp, nil
}

func makeL1Dir(dbPath string) string {
	return filepath.Join(dbPath, lv1DirName)
}

func makeL1SegDir(l1Dir string) string {
	return filepath.Join(l1Dir, lv1SegsDirName)
}

func (l *lv1) makeSegFile() (f vfs.File, id int64, err error) {

	id = l.nextSegID
	defer func() {
		l.nextSegID++
	}()

	fp := makeSegPath(l.segsPath, id)

	f, err = l.fs.Create(fp)
	if err != nil {
		return nil, id, err
	}

	err = vfs.TryFAlloc(f, l.segSize)
	if err != nil {
		return
	}

	return f, id, nil
}

func makeSegPath(segsPath string, id int64) string {
	return filepath.Join(segsPath, strconv.FormatInt(id, 10)+segSuffix)
}

func makeSegIdxPath(segsPath string, id int64) string {
	return filepath.Join(segsPath, strconv.FormatInt(id, 10)+segIdxSuffix)
}

func (l *lv1) makeSeg(snap *pebble.Snapshot, dirtyCnt int) error {

	var err error
	var id int64
	var segF, segIdxF vfs.File
	defer func() {
		if err != nil {
			err = xerrors.WithMessage(err, "failed to make seg")
			xlog.Error(err.Error())
			if segF != nil {
				_ = segF.Close()
			}
			if segIdxF != nil {
				_ = segIdxF.Close()
			}
			cleanDirtySeg(l.fs, l.segsPath, id)
		}
	}()

	segF, id, err = l.makeSegFile()
	if err != nil {
		return err
	}

	// Set in Lv0 maybe much faster than transferring to lv1,
	// dirtyCnt will be larger than DefaultToLv1MaxEntries in production env.
	items := make([]index.OffsetIndexItem, 0, dirtyCnt)
	iter := snap.NewIter(nil)

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()

		items = append(items, index.OffsetIndexItem{Key: string(k)})
	}
	err = iter.Close()
	if err != nil {
		return err
	}

	sort.Sort(idxItems(items)) // Sort by key in asc order.
	// 1. items first and last be segment in seg tree

	buf := directio.AlignedBlock(4*1024*1024 + 4*1024) // Max key+value. If larger, will allocate new space.

	// 1. offset align to 8 KB
	// 2. combine key + value, then send to scheduler
	// 3. after all traSnsfer
	// 4. delete one by one in lv0
	// 5. write down lv1 index
	// 6. update index in lv1

	itemOff := lv1BlockCntSize
	written := int64(0)
	offset := int64(0) // offset from segment file.

	itemsInBlock := 0
	hs := make([]uint64, 0, 8)
	offs := make([]uint16, 0, 8)
	sizes := make([]uint32, 0, 8)

	for _, item := range items {

		if xbytes.AlignSize(written, directio.AlignSize) >= lv1FirstBlockSize {

			makeFirstBlockHeader(buf, itemsInBlock, hs, offs, sizes)

			err = l.sched.DoSync(xio.ReqObjWrite, segF, offset, buf[:xbytes.AlignSize(written, directio.AlignSize)])
			if err != nil {
				return err
			}

			itemsInBlock = 0
			hs = hs[:0]
			offs = offs[:0]
			sizes = sizes[:0]
			written = 0
		}

		kbs := xstrconv.ToBytes(item.Key)
		val, closer, err := snap.Get(kbs)
		if err != nil {
			return err
		}
		itemOff += 8 // hash
		itemOff += 6 // offset + size
		itemOff += len(kbs)
		itemOff += len(val)

		written += itemOff
	}

}

func (l *lv1) fillIdx(items []index.OffsetIndexItem, snap *pebble.Snapshot)

type idxItems []index.OffsetIndexItem

func (e idxItems) Len() int {
	return len(e)
}

func (e idxItems) Less(i, j int) bool {

	return e[i].Key < e[j].Key
}

func (e idxItems) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func cleanDirtySeg(fs vfs.FS, segsPath string, id int64) {

	if id == 0 {
		return
	}

	sp := makeSegPath(segsPath, id)
	sip := makeSegIdxPath(segsPath, id)

	_ = fs.Remove(sp)
	_ = fs.Remove(sip)
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
	idxFn := filepath.Join(l.path, lv1SegsDirName, tmpDir,
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
