package neo

import (
	"encoding/binary"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
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
	nextSegID int64            // start from 0.
	segs      []unsafe.Pointer // *vfs.DirectFile.
	indexes   []unsafe.Pointer // *index.SlimIndex

	segsPath string

	sched xio.Scheduler
	fs    vfs.FS

	ranges unsafe.Pointer // *bsegtree.BSTree
}

const (
	segBufSize = 512 * 1024

	version1           = uint32(1) // In present, we only has one version.
	chunkIdxHeaderSize = 4 * 1024
)

func createLv1(dbPath string, fs vfs.FS, sched xio.Scheduler) (l *lv1, err error) {

	sp, err := createLv1Paths(dbPath, fs)
	if err != nil {
		return nil, err
	}

	t := new(bsegtree.BSTree)
	t.Clear()

	l = &lv1{
		nextSegID: 0,
		indexes:   make([]unsafe.Pointer, 1024, 1024),
		segsPath:  sp,
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

func (l *lv1) makeSegFile(minSize int64) (f vfs.File, id int64, err error) {

	id = l.nextSegID
	defer func() {
		if err == nil {
			l.nextSegID++
		}
	}()

	fp := makeSegPath(l.segsPath, id)

	f, err = l.fs.Create(fp)
	if err != nil {
		return nil, id, err
	}

	err = vfs.TryFAlloc(f, minSize) // Ensure has enough space.
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

// makeSegIdx write down segment and making segment index.
// segment file will be added in memory.
//
// minSize is key + value size for this snapshot counted by neo.
func (l *lv1) makeSegIdx(snap *pebble.Snapshot, dirtyCnt int, minSize int64) (
	id int64, idx *index.SlimIndex, min, max string, err error) {

	var segF vfs.File
	defer func() {
		if err != nil {
			err = xerrors.WithMessage(err, "failed to make seg")
			xlog.Error(err.Error())
			if segF != nil {
				_ = segF.Close()
			}
			cleanDirtySeg(l.fs, l.segsPath, id)
		}
	}()

	segF, id, err = l.makeSegFile(minSize)
	if err != nil {
		return
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
		return
	}

	sort.Sort(idxItems(items)) // Sort by key in asc order.

	buf := directio.AlignedBlock(4*1024*1024 + 2*lv1BlockMinSize) // Max space wil be taken by one item.

	itemOff := lv1BlockHeaderSize // This item offset from first byte of a block.
	offset := int64(0)            // offset from segment file.

	itemsInBlock := 0
	hs := make([]uint64, 0, lv1BlockMaxItems)
	offs := make([]uint16, 0, lv1BlockMaxItems)
	sizes := make([]uint32, 0, lv1BlockMaxItems)
	itemsIdxInBlock := make([]int, 0, lv1BlockMaxItems)

	for i, item := range items {

		wa := xbytes.AlignSize(int64(itemOff), directio.BlockSize)

		if wa >= lv1BlockMinSize || itemsInBlock == 8 {

			makeLv1MinBlock(buf, itemsInBlock, hs, offs, sizes)

			err = l.sched.DoSync(xio.ReqObjWrite, segF, offset, buf[:wa])
			if err != nil {
				return
			}

			for _, idx := range itemsIdxInBlock {
				items[idx].Offset = offset
			}

			itemOff = lv1BlockHeaderSize
			offset += wa

			itemsInBlock = 0
			hs = hs[:0]
			offs = offs[:0]
			sizes = sizes[:0]
			itemsIdxInBlock = itemsIdxInBlock[:0]
		}

		kbs := xstrconv.ToBytes(item.Key)
		val, closer, err := snap.Get(kbs)
		if err != nil {
			return 0, nil, "", "", err
		}

		offs = append(offs, uint16(itemOff))

		copy(buf[itemOff:], kbs)
		itemOff += len(kbs)
		copy(buf[itemOff:], val)
		itemOff += len(val)

		itemsInBlock++
		hs = append(hs, xxhash.Sum64(kbs))
		sizes = append(sizes, uint32(len(val)))
		itemsIdxInBlock = append(itemsIdxInBlock, i)

		_ = closer.Close()
	}

	idx, err = index.NewSlimIndex(items, nil)
	if err != nil {
		return
	}

	atomic.StorePointer(&l.segs[id], unsafe.Pointer(segF.(*vfs.DirectFile)))

	return id, idx, items[0].Key, items[len(items)-1].Key, nil
}

// addSeg adds its index to lv1.
// index on local:
// 4KB header + index_bytes
func (l *lv1) addSegIdx(id int64, idx *index.SlimIndex, min, max []byte) error {

	idxFp := makeSegIdxPath(l.segsPath, id)

	f, err := l.fs.Create(idxFp)
	if err != nil {
		return err
	}
	defer f.Close()

	idxBytes, err := idx.Marshal()
	if err != nil {
		return err
	}

	buf := xbytes.GetAlignedBytes(segBufSize)
	defer xbytes.PutBytes(buf)

	makeSegIdxHeader(version1, xxhash.Sum64(idxBytes), uint64(len(idxBytes)), min, max, buf)
	undone := copy(buf[chunkIdxHeaderSize:], idxBytes)
	done, off := 0, int64(0)
	for done < len(idxBytes) {

		err = l.sched.DoSync(xio.ReqChunkWrite, f, off, buf)
		if err != nil {
			return err
		}

		done += undone

		undone = copy(buf, idxBytes[done:])
		off += segBufSize
	}

	atomic.StorePointer(&l.indexes[id], unsafe.Pointer(idx))

	var st *bsegtree.BSTree
	ost := atomic.LoadPointer(&l.ranges)
	if ost == nil {
		st = bsegtree.New().(*bsegtree.BSTree)
	} else {
		st = (*bsegtree.BSTree)(ost).Clone().(*bsegtree.BSTree)
	}
	st.Push(min, max)
	st.Build()

	atomic.StorePointer(&l.ranges, unsafe.Pointer(st))

	return nil
}

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

func makeSegIdxHeader(version uint32, checksum uint64, idxSize uint64, min, max, buf []byte) {

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

func parseSegIdxHeader(buf []byte) (version uint32, checksum uint64, idxSize uint64, err error) {

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
