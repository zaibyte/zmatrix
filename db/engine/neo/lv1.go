package neo

import (
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/spf13/cast"

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
	segBufSize = 512 * 1024 // write buffer
	// For 4 millions segment (the biggest segment), the index may take 4.5 MiB space.
	// 8 MB buf is enough for loading.
	segIdxLoadBufSize = 8 * 1024 * 1024

	// It's segment index version.
	// version1 means using slim trie as index.
	version1         = uint32(1) // In present, we only has one version.
	segIdxHeaderSize = 4 * 1024
)

// createLV1 creates new lv1 with empty segment.
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

// After createLv1, load lv1 from disk.
// dbPath must have been checked.
func (l *lv1) load() (err error) {

	fs := l.fs
	segDir := l.segsPath
	// 1. list all file
	// 2. get seg & idx pairs
	// 3. remove all non-pair seg/idx files
	// 4. add seg & idx to lv1
	ns, err := fs.List(segDir)
	if err != nil {
		return err
	}

	segs := make(map[string]struct{})
	idxs := make(map[string]struct{})

	waitRemoved := make([]string, 0, len(ns))

	for _, n := range ns {

		shouldRm := true
		if strings.HasSuffix(n, segSuffix) {

			ids := strings.TrimSuffix(n, segSuffix)
			_, err := cast.ToInt64E(ids)
			if err == nil {
				shouldRm = false
				segs[ids] = struct{}{}
			}

		} else if strings.HasSuffix(n, segIdxSuffix) {
			ids := strings.TrimSuffix(n, segIdxSuffix)
			_, err := cast.ToInt64E(ids)
			if err == nil {
				shouldRm = false
				idxs[ids] = struct{}{}
			}
		}

		if shouldRm {
			waitRemoved = append(waitRemoved, n)
		}
		shouldRm = true
	}

	for k := range segs {
		if _, ok := idxs[k]; !ok {
			waitRemoved = append(waitRemoved, k+segSuffix)
		}
	}
	for k := range idxs {
		if _, ok := segs[k]; !ok {
			waitRemoved = append(waitRemoved, k+segIdxSuffix)
		}
	}

	for _, rm := range waitRemoved {
		_ = fs.Remove(filepath.Join(segDir, rm))
	}

	idxBuf := directio.AlignedBlock(segIdxLoadBufSize)
	for ids := range segs {
		err = l.loadSeg(ids, idxBuf)
		if err != nil {
			err = xerrors.WithMessage(err, fmt.Sprintf("failed to load seg: %s", ids))
			return err
		}
	}
	return nil
}

func (l *lv1) loadSeg(ids string, buf []byte) error {

	id := cast.ToInt64(ids)

	segFn := filepath.Join(l.segsPath, ids+segSuffix)
	idxFn := filepath.Join(l.segsPath, ids+segIdxSuffix)

	segF, err := l.fs.Open(segFn)
	if err != nil {
		return err
	}
	l.addSeg(id, segF)

	idx, min, max, err := loadSegIdxFromFile(l.fs, idxFn, buf)
	if err != nil {
		return err
	}

	l.addSegIdx(id, idx, min, max)
	return nil
}

// search key's value if existed.
func (l *lv1) search(key []byte) (value []byte, closer io.Closer, err error) {

	ids, ok := l.searchSeg(key)
	if !ok {
		return nil, nil, orpc.ErrNotFound
	}

	for _, id := range ids {
		segF, idx, ok := l.getSeg(id)
		if ok {
			o, found := idx.SlimTrie.RangeGet(xstrconv.ToString(key))
			if !found {
				continue
			}
			offset := o.(int64)
			has, value, closer, err := l.getValueFromSeg(segF, offset, key)
			if err != nil {
				xlog.Errorf("failed to read seg: %s", err.Error())
				continue
			}
			if !has {
				continue
			}
			return value, closer, nil
		}
	}
	return nil, nil, orpc.ErrNotFound
}

func (l *lv1) getValueFromSeg(f vfs.File, offset int64, key []byte) (bool, []byte, io.Closer, error) {

	buf := xbytes.GetAlignedBytes(lv1BlockAlignSize)
	_, err := f.ReadAt(buf, offset)
	if err != nil {
		xbytes.PutAlignedBytes(buf)

		return false, nil, nil, err
	}

	off, size, ok := searchInBlock(buf, key)
	if !ok {
		xbytes.PutAlignedBytes(buf)
		return false, nil, nil, nil
	}

	start := int(off)
	end := start + int(size)

	if end <= lv1BlockAlignSize { // It's in this block.
		return true, buf[start:end], xbytes.PoolAlignedBytesCloser{P: buf}, nil
	}

	xbytes.PutAlignedBytes(buf)

	// sorry for allocating again :D
	bigB := xbytes.GetAlignedBytes(int(xbytes.AlignSize(lv1BlockAlignSize+int64(size), 4096)))

	_, err = f.ReadAt(bigB, offset)
	if err != nil {
		xbytes.PutAlignedBytes(bigB)
		return false, nil, nil, err
	}

	return true, bigB[start:end], xbytes.PoolAlignedBytesCloser{P: bigB}, nil
}

// getSeg gets segment file & its index from memory.
func (l *lv1) getSeg(id int) (segF vfs.File, idx *index.SlimIndex, ok bool) {

	sp := atomic.LoadPointer(&l.segs[id])
	if sp == nil {
		return nil, nil, false
	}
	ip := atomic.LoadPointer(&l.indexes[id])
	if ip == nil {
		return nil, nil, false
	}

	return (*vfs.DirectFile)(sp), (*index.SlimIndex)(ip), true
}

// searchSeg tries to find out which segment the key belongs to.
func (l *lv1) searchSeg(key []byte) (ids []int, ok bool) {

	btp := atomic.LoadPointer(&l.ranges)

	if btp == nil {
		return nil, false
	}

	bt := (*bsegtree.BSTree)(btp)

	ids = bt.QueryPoint(key)
	if len(ids) == 0 {
		return nil, false
	}
	return ids, true
}

// createLv1Paths create paths needed by lv1, and return segments paths.
func createLv1Paths(dbPath string, fs vfs.FS) (string, error) {
	dir := makeL1Dir(dbPath)
	err := fs.MkdirAll(dir, 0755)
	if vfs.IsExist(err) {
		err = nil
	}
	if err != nil {
		return "", err
	}

	sp := makeL1SegDir(dir)
	err = fs.MkdirAll(sp, 0755)
	if vfs.IsExist(err) {
		err = nil
	}
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

	if id >= 1024 {
		return nil, 0, ErrLv1SegmentsFull
	}

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

	l.addSeg(id, segF)

	return id, idx, items[0].Key, items[len(items)-1].Key, nil
}

func (l *lv1) addSeg(id int64, segF vfs.File) {
	atomic.StorePointer(&l.segs[id], unsafe.Pointer(segF.(*vfs.DirectFile)))
}

func (l *lv1) addSegIdx(id int64, idx *index.SlimIndex, min, max []byte) {
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
}

// addSeg adds its index to lv1.
// index on local:
// 4KB header + index_bytes
func (l *lv1) persistIdx(id int64, idx *index.SlimIndex, min, max []byte) error {

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
	undone := copy(buf[segIdxHeaderSize:], idxBytes)
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

	l.addSegIdx(id, idx, min, max)

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

func makeSegIdxHeader(version uint32, idxChecksum uint64, idxSize uint64, min, max, buf []byte) {

	binary.LittleEndian.PutUint32(buf, version)
	binary.LittleEndian.PutUint64(buf[4:], idxChecksum)
	binary.LittleEndian.PutUint64(buf[12:], idxSize)
	binary.LittleEndian.PutUint32(buf[20:], uint32(len(min))) // longest key size is 255 bytes.
	copy(buf[24:], min)
	nextOff := 24 + len(min)
	binary.LittleEndian.PutUint32(buf[nextOff:], uint32(len(max)))
	nextOff += 4
	copy(buf[nextOff:], max)

	binary.LittleEndian.PutUint32(buf[segIdxHeaderSize-4:], 0)

	headerChecksum := xdigest.Sum32(buf[:segIdxHeaderSize])
	binary.LittleEndian.PutUint32(buf[segIdxHeaderSize-4:], headerChecksum)
}

func parseSegIdxHeader(buf []byte) (version uint32, checksum uint64, idxSize uint64, min, max []byte, err error) {

	c := binary.LittleEndian.Uint32(buf[segIdxHeaderSize-4:])

	binary.LittleEndian.PutUint32(buf[segIdxHeaderSize-4:], 0)
	headerChecksum := xdigest.Sum32(buf[:segIdxHeaderSize])
	if c != headerChecksum {
		err = xerrors.WithMessage(orpc.ErrChecksumMismatch, "failed to parse chunk index header")
		return
	}

	version = binary.LittleEndian.Uint32(buf[:4])
	checksum = binary.LittleEndian.Uint64(buf[4:12])
	idxSize = binary.LittleEndian.Uint64(buf[12:20])

	minSize := binary.LittleEndian.Uint32(buf[20:24])
	min = make([]byte, minSize)
	copy(min, buf[24:])

	maxSize := binary.LittleEndian.Uint32(buf[24+minSize:])
	max = make([]byte, maxSize)
	copy(max, buf[28+minSize:])

	return
}

func loadSegIdxFromFile(fs vfs.FS, fp string, buf []byte) (idx *index.SlimIndex, min, max []byte, err error) {

	f, err := fs.Open(fp)
	if err != nil {
		return nil, nil, nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, nil, nil, err
	}
	size := fi.Size()
	if size > int64(len(buf)) {
		buf = directio.AlignedBlock(int(size))
	} else {
		buf = buf[:size]
	}

	_, err = f.ReadAt(buf, 0)
	if err != nil {
		return nil, nil, nil, err
	}

	var checksum, idxSize uint64
	_, checksum, idxSize, min, max, err = parseSegIdxHeader(buf[:segIdxHeaderSize])
	if err != nil {
		return nil, nil, nil, err
	}

	if checksum != xxhash.Sum64(buf[segIdxHeaderSize:segIdxHeaderSize+idxSize]) {
		err = xerrors.WithMessage(orpc.ErrChecksumMismatch, fmt.Sprintf("failed load seg: %s idx", fp))
		return
	}

	err = idx.Unmarshal(buf[segIdxHeaderSize : segIdxHeaderSize+idxSize])
	if err != nil {
		return nil, nil, nil, err
	}
	return
}
