package neo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"unsafe"

	"g.tesamc.com/IT/zaipkg/xmath"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zaipkg/xstrconv"
	"g.tesamc.com/IT/zmatrix/pkg/zmerrors"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
	"github.com/openacid/slim/encode"
	"github.com/openacid/slim/index"
	"github.com/openacid/slim/trie"
	"github.com/spf13/cast"
	"github.com/templexxx/bsegtree"
	"github.com/templexxx/xorsimd"
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
	maxSegs    = 1024
	segBufSize = 512 * 1024 // write buffer
	// For 4 millions segment (the biggest segment), the index may take 4.5 MiB space.
	// 8 MB buf is enough for loading.
	segIdxLoadBufSize = 8 * 1024 * 1024

	// It's segment index version.
	// segIdxVersion1 means using slim trie as index.
	segIdxVersion1   = uint32(1) // In present, we only has one version.
	segIdxHeaderSize = 4 * 1024
)

func createOrLoadLv1(dbPath string, fs vfs.FS, sched xio.Scheduler, isCreate bool) (l *lv1, err error) {

	sp, err := createLv1Paths(dbPath, fs, isCreate)
	if err != nil {
		return nil, err
	}

	t := new(bsegtree.BSTree)
	t.Clear()

	l = &lv1{
		nextSegID: 0,
		segs:      make([]unsafe.Pointer, maxSegs, maxSegs),
		indexes:   make([]unsafe.Pointer, maxSegs, maxSegs),
		segsPath:  sp,
		sched:     sched,
		fs:        fs,
		ranges:    unsafe.Pointer(t),
	}

	return
}

func loadLv1(dbPath string, fs vfs.FS, sched xio.Scheduler) (l *lv1, err error) {

	l, err = createOrLoadLv1(dbPath, fs, sched, false)
	if err != nil {
		return nil, err
	}

	err = l.load()
	if err != nil {
		l.close()
		return nil, err
	}

	btp := atomic.LoadPointer(&l.ranges)

	bt := (*bsegtree.BSTree)(btp)

	xlog.Debugf("all ranges: %v", bt.GetAll())

	return
}

// createLV1 creates new lv1 with empty segment.
func createLv1(dbPath string, fs vfs.FS, sched xio.Scheduler) (l *lv1, err error) {

	return createOrLoadLv1(dbPath, fs, sched, true)
}

// After createLv1, load lv1 from disk.
// dbPath must have been checked.
func (l *lv1) load() (err error) {

	// 1. list all file
	// 2. get seg & idx pairs
	// 3. remove all non-pair seg/idx files
	// 4. add seg & idx to lv1
	fs := l.fs
	segDir := l.segsPath

	ns, err := fs.List(segDir)
	if err != nil {
		return err
	}

	maxSegID, err := l.findSegPairs(ns)
	if err != nil {
		return err
	}
	if maxSegID == -1 {
		return nil
	}

	idxBuf := directio.AlignedBlock(segIdxLoadBufSize)
	for id := 0; id <= maxSegID; id++ {

		err = l.loadSeg(int64(id), idxBuf)
		if err != nil {
			if errors.Is(err, orpc.ErrChecksumMismatch) && id == maxSegID { // Regard as short-write caused by interruption.

				_ = l.fs.RemoveAll(makeSegPath(l.segsPath, int64(id)))
				_ = l.fs.RemoveAll(makeSegIdxPath(l.segsPath, int64(id)))

				err = nil
			}
		}
		if err != nil { // Non checksum error.
			err = xerrors.WithMessage(err, fmt.Sprintf("failed to load seg: %d", id))
			return err
		}
		xlog.Debugf("load lv1 seg ok: %s", filepath.Join(l.segsPath, cast.ToString(id)))
	}

	l.nextSegID = int64(maxSegID) + 1

	return nil
}

// findSegPairs find out seg & index file pairs by checking filenames.
//
// It will check segment file & index file pairs from seg_0 until reach the last one.
// Only last segment file could have incomplete/non-existed index file.
func (l *lv1) findSegPairs(ns []string) (maxSegID int, err error) {

	allFns := make(map[string]struct{})
	for _, fn := range ns {
		allFns[filepath.Join(l.segsPath, fn)] = struct{}{}
	}

	// 1. find max segment file
	maxSegID = -1
	for i := 0; i < maxSegs; i++ {
		segFn := makeSegPath(l.segsPath, int64(i))
		if _, ok := allFns[segFn]; ok {
			maxSegID = i
		} else {
			break
		}
	}

	if maxSegID == -1 {
		return
	}

	// 2. check maxSegID's index
	if _, ok := allFns[makeSegIdxPath(l.segsPath, int64(maxSegID))]; !ok {
		maxSegID--
	}

	// 3. segment file & index must be existed in [0, maxSegID]
	reserved := make(map[string]struct{})
	for i := 0; i <= maxSegID; i++ {
		idxFn := makeSegIdxPath(l.segsPath, int64(i))
		if _, ok := allFns[idxFn]; !ok {
			return maxSegID, zmerrors.ErrDatabaseBroken
		}
		reserved[idxFn] = struct{}{}
		reserved[makeSegPath(l.segsPath, int64(i))] = struct{}{}
	}

	// 4. clean up all files which aren't segment / index.
	for fn := range allFns {
		if _, ok := reserved[fn]; !ok {
			_ = l.fs.RemoveAll(fn)
		}
	}

	return maxSegID, nil
}

func (l *lv1) loadSeg(id int64, buf []byte) error {

	ids := cast.ToString(id)

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

	l.addSegIdxRange(id, idx, min, max)
	return nil
}

func (l *lv1) has(key []byte) bool {
	ids, ok := l.searchSeg(key)
	if !ok {
		return false
	}

	for _, id := range ids {
		_, idx, got := l.getSeg(id)
		if got { // Ensure there is no memory order issues.
			_, found := idx.SlimTrie.RangeGet(xstrconv.ToString(key))
			if !found {
				continue
			} else {
				return true // SlimTrie has extremely low false positive rate. Avoiding disk scan by return true directly.
			}
		}
	}
	return false
}

// search key's value if existed.
func (l *lv1) search(key []byte) (value []byte, closer io.Closer, err error) {

	// start := tsc.UnixNano()
	ids, ok := l.searchSeg(key)
	if !ok {
		xlog.Warnf("failed to search seg in ranges: not found")
		return nil, nil, orpc.ErrNotFound
	}

	// xlog.Debugf("cost: %.2fus for search seg for %d",
	//	(float64(tsc.UnixNano())-float64(start))/1000, binary.BigEndian.Uint64(key))

	var has bool
	for _, id := range ids {

		xlog.Debugf("found key: %d in seg: %d", bsegtree.AbbreviatedKey(key), id)

		segF, idx, ok := l.getSeg(id)
		if ok { // Ensure there is no memory order issues.
			o, found := idx.SlimTrie.RangeGet(xstrconv.ToString(key))
			if !found {
				xlog.Warnf("failed to search trie tree: not found key: %d", bsegtree.AbbreviatedKey(key))
				continue
			}
			// xlog.Debugf("cost: %.2fus for search seg & index for %d",
			//	(float64(tsc.UnixNano())-float64(start))/1000, binary.BigEndian.Uint64(key))
			offset := o.(int64)

			has, value, closer, err = l.getValueFromSeg(segF, offset, key)
			if err != nil || !has {
				if err == nil && !has {
					err = orpc.ErrNotFound
				}
				xlog.Warnf("failed to search from seg file: %s", err.Error())
				continue
			}

			// xlog.Debugf("cost: %.2fus for found %d in %d segs",
			//	(float64(tsc.UnixNano())-float64(start))/1000, binary.BigEndian.Uint64(key), len(ids))
			return value, closer, nil
		}
	}
	return nil, nil, orpc.ErrNotFound
}

func (l *lv1) getValueFromSeg(f vfs.File, offset int64, key []byte) (bool, []byte, io.Closer, error) {

	// startT := tsc.UnixNano()

	buf := xbytes.GetAlignedBytes(blockGainSize)

	err := l.sched.DoSync(xio.ReqObjRead, f, offset, buf)
	if err != nil {
		xbytes.PutAlignedBytes(buf)

		return false, nil, nil, err
	}

	// xlog.Debugf("cost: %.2fus for read value of %d in file",
	//	(float64(tsc.UnixNano())-float64(startT))/1000, binary.BigEndian.Uint64(key))

	off, size := searchInLv1Block(buf, key)
	if off == -1 {
		xbytes.PutAlignedBytes(buf)
		return false, nil, nil, nil
	}

	// xlog.Debugf("cost: %.2fus for read + search value of %d in file",
	//	(float64(tsc.UnixNano())-float64(startT))/1000, binary.BigEndian.Uint64(key))

	start := off
	end := start + size

	if end <= blockGainSize { // It's in this block.
		//	xlog.Debugf("cost: %.2fus for read small value in block of %d in file",
		//		(float64(tsc.UnixNano())-float64(startT))/1000, binary.BigEndian.Uint64(key))
		return true, buf[start:end], xbytes.PoolAlignedBytesCloser{P: buf}, nil
	}

	left := size - (blockGainSize - start)
	wanted := xbytes.AlignSize(int64(left), 4096)
	var bigB []byte
	bigBSize := blockGainSize + wanted // Big enough.
	var closer io.Closer
	if bigBSize > xbytes.MaxSizeInPool {
		bigB = directio.AlignedBlock(int(bigBSize))
		closer = io.NopCloser(nil)
	} else {
		bigB = xbytes.GetAlignedBytes(int(bigBSize))
		closer = xbytes.PoolAlignedBytesCloser{P: bigB}
	}

	err = l.sched.DoSync(xio.ReqObjRead, f, offset+blockGainSize, bigB[blockGainSize:])
	if err != nil {
		_ = closer.Close()
		xbytes.PutAlignedBytes(buf)

		return false, nil, nil, err
	}

	copy(bigB[start:blockGainSize], buf[start:blockGainSize])
	xbytes.PutAlignedBytes(buf)

	v := bigB[start : blockGainSize+left]

	return true, v, closer, nil
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
func createLv1Paths(dbPath string, fs vfs.FS, isCreate bool) (string, error) {
	dir := makeL1Dir(dbPath)
	if isCreate {
		_ = fs.RemoveAll(dir)
		err := fs.MkdirAll(dir, 0755)
		if err != nil {
			return "", err
		}
	} else {
		if !vfs.IsDirExisted(fs, dir) {
			return "", orpc.ErrNotFound
		}
	}

	sp := makeL1SegDir(dir)
	err := fs.MkdirAll(sp, 0755)
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

	if id >= maxSegs {
		return nil, 0, xerrors.WithMessage(zmerrors.ErrDatabaseFull, "lv1 is full of 1024 segments")
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
	id int64, idx *index.SlimIndex, min, max []byte, added int, err error) {

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

	// We're using directIO, must be aligned to page size.
	buf := directio.AlignedBlock(int(maxBlockSize)) // Max space will be taken by one item.
	bufSize := len(buf)

	offBuf := 0            // offset in buf.
	offWritten := int64(0) // written offset in seg file.

	existed := 0 // existed key in lv1, jump over these items.

	lastEntryAlignGain := int64(0) // last entry starts at offset which aligned gain.

	maxIdx := -1
	for j, item := range items {

		key := xstrconv.ToBytes(item.Key)

		if l.has(key) {
			existed++
			continue
		}

		if min == nil {
			min = make([]byte, len(item.Key))
			copy(min, item.Key)
		}

		maxIdx = j

		val, closer, err2 := snap.Get(key)
		if err2 != nil {
			err = err2
			return
		}

		nk := len(item.Key)
		nv := len(val)

		// Offset in seg file aligned to gain.
		itemOff := xmath.AlignToLast(offWritten+int64(offBuf), blockGainSize)
		valOff := xmath.AlignToLast(offWritten+int64(offBuf)+valLenInBlock+keyLenInBlock+int64(nk), blockGainSize)

		originOffBuf := offBuf
		if itemOff != valOff { // Expect key_len, val_len, key are in the same gain.
			offBuf = int(xmath.AlignToLast(int64(offBuf), blockGainSize)) + blockGainSize // Move item to next gain.
		}

		itemOff = xmath.AlignToLast(offWritten+int64(offBuf), blockGainSize)
		if itemOff != offWritten+int64(offBuf) { // offBuf is not aligned to gain.
			if itemOff != lastEntryAlignGain { // Move to next gain if itemOff is not started with an entry.
				offBuf = int(xmath.AlignToLast(int64(offBuf), blockGainSize)) + blockGainSize
			}
		}

		// itemOff(offBuf) could be satisfied with these properties now:
		// itemOff is started with an entry (this one or another one which is in the same gain with this item).

		itemSizeInBlock := valLenInBlock + keyLenInBlock + nk + nv

		if offBuf+itemSizeInBlock > bufSize { // Cannot write to buf anymore.

			wn := xbytes.AlignSize(int64(originOffBuf), blockGainSize)

			xorsimd.Bytes(buf[originOffBuf:wn], buf[originOffBuf:wn], buf[originOffBuf:wn]) // Clean up extra bytes.

			err = l.sched.DoSync(xio.ReqChunkWrite, segF, offWritten, buf[:wn]) // Using chunk write, because it's big I/O in most cases.
			if err != nil {
				return
			}

			offWritten += wn
			offBuf = 0 // Next round will begin with the beginning.
		}

		itemOff = xmath.AlignToLast(offWritten+int64(offBuf), blockGainSize)

		added++

		n := makeLv1Block(buf[offBuf:], nk, nv, key, val)

		items[j].Offset = itemOff

		_ = closer.Close()

		if xmath.AlignToLast(offWritten+int64(offBuf), blockGainSize) == offWritten+int64(offBuf) { // offset is aligned to gain.
			lastEntryAlignGain = offWritten + int64(offBuf)
		}

		offBuf += n
	}

	segFSize := offWritten

	if offBuf != 0 { // if there is un-flushed in buf
		wn := xbytes.AlignSize(int64(offBuf), blockGainSize)

		xorsimd.Bytes(buf[offBuf:wn], buf[offBuf:wn], buf[offBuf:wn]) // Clean up extra bytes.

		err = l.sched.DoSync(xio.ReqChunkWrite, segF, offWritten, buf[:wn]) // Using chunk write, because it's big I/O in most cases.
		if err != nil {
			return
		}
		segFSize += wn
	}

	if added == 0 {
		return id, nil, nil, nil, added, nil
	}

	idx, err = index.NewSlimIndex(items, nil)
	if err != nil {
		return
	}

	l.addSeg(id, segF)

	xlog.Infof("persist seg file done for seg_id: %d with %d items in %.2fMB",
		id, len(items)-existed, float64(segFSize)/float64(1024*1024))

	max = []byte(items[maxIdx].Key) // Must have, because added != 0.

	return id, idx, min, max, added, nil
}

func (l *lv1) addSeg(id int64, segF vfs.File) {
	atomic.StorePointer(&l.segs[id], unsafe.Pointer(segF.(*vfs.DirectFile)))
}

// addSegIdxRange adds new index & range to lv1.
// must be invoked by segment id asc order.
func (l *lv1) addSegIdxRange(id int64, idx *index.SlimIndex, min, max []byte) {

	l.addSegIdx(id, idx)
	// addRange must be the last operation of add seg, add segIdx, addRange.
	// Otherwise, we may search the right seg but without seg file & seg index result. (on X86, Stores are not reordered with other stores)
	l.addRange(min, max)

	xlog.Infof("seg idx & range [%d, %d] added: %d", bsegtree.AbbreviatedKey(min), bsegtree.AbbreviatedKey(max), id)
}

func (l *lv1) addSegIdx(id int64, idx *index.SlimIndex) {
	atomic.StorePointer(&l.indexes[id], unsafe.Pointer(idx))
}

func (l *lv1) addRange(min, max []byte) {
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
// 4 KB header + index_bytes
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

	buf := directio.AlignedBlock(int(xbytes.AlignSize(int64(len(idxBytes)+segIdxHeaderSize), 4096)))

	makeSegIdxHeader(segIdxVersion1, xxhash.Sum64(idxBytes), uint64(len(idxBytes)), min, max, buf)
	copy(buf[segIdxHeaderSize:], idxBytes)

	err = l.sched.DoSync(xio.ReqChunkWrite, f, 0, buf)
	if err != nil {
		return err
	}

	l.addSegIdxRange(id, idx, min, max)

	xlog.Infof("persist seg idx done for seg_id: %d in %.2fMB",
		id, float64(len(buf))/float64(1024*1024))

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

	st, err := trie.NewSlimTrie(encode.I64{}, nil, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	err = st.Unmarshal(buf[segIdxHeaderSize : segIdxHeaderSize+idxSize])
	if err != nil {
		return nil, nil, nil, err
	}
	idx = &index.SlimIndex{SlimTrie: *st}
	return
}

func (l *lv1) close() {

	for i := range l.segs {
		segF, _, ok := l.getSeg(i)
		if ok {
			_ = segF.Close()
		}
	}
}
