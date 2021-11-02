package neo

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/cespare/xxhash/v2"
)

const (
	lv1BlockCntSize    = 2
	lv1BlockHeaderSize = 114
	lv1BlockMaxItems   = 8
	lv1BlockAlignSize  = 8 * 1024 // 8 KiB could get the best balance between index memory overhead & small items random access performance.
	lv1BlockMinSize    = lv1BlockAlignSize
)

func makeLv1MinBlock(buf []byte, cnt int, hs []uint64, offs []uint16, sizes []uint32) {

	off := lv1BlockCntSize
	binary.LittleEndian.PutUint16(buf[:off], uint16(cnt))
	for i := range hs {
		binary.LittleEndian.PutUint64(buf[off:off+8], hs[i])
		off += 8
	}
	off = lv1BlockCntSize + 8*8
	for i := range offs {
		binary.LittleEndian.PutUint16(buf[off:off+2], offs[i])
		off += 2
		binary.LittleEndian.PutUint32(buf[off:off+4], sizes[i])
		off += 4
	}
}

// searchInBlock searches the block of a key (returned by index),
// return offset from the first byte of block, and value size if found (ok will be true).
//
// see README.md for the details of block.
func searchInBlock(block, key []byte) (offset uint16, size uint32, ok bool) {

	cnt := binary.LittleEndian.Uint16(block[:lv1BlockCntSize])
	if cnt == 1 {
		offset, size = getOffsetSizeFromBlock(block, 0)
		offset += uint16(len(key))
		ok = true
		return
	}

	h := xxhash.Sum64(key)

	ns, ok := getNFromBlock(block, int(cnt), h)
	if !ok {
		return 0, 0, false
	}
	defer intsPool.Put(ns[:0])

	for _, n := range ns {
		offset, size = getOffsetSizeFromBlock(block, n)
		if bytes.Equal(block[offset:int(offset)+len(key)], key) {
			return offset + uint16(len(key)), size, true
		}
	}
	return 0, 0, false
}

var intsPool = sync.Pool{New: func() interface{} {
	return make([]int, 0, 8)
}}

// getNFromBlock gets positions which have the same hash.
// Don't forget to put back ints to pool.
func getNFromBlock(block []byte, cnt int, h uint64) (ns []int, ok bool) {

	off := lv1BlockCntSize

	ns = intsPool.Get().([]int)[:0]

	for i := 0; i < lv1BlockMaxItems; i++ {
		if h == binary.LittleEndian.Uint64(block[off:]) {
			ns = append(ns, i)
		}
		off += 8
	}

	if len(ns) == 0 {
		intsPool.Put(ns[:0])
		return nil, false
	}

	return ns, true
}

// n is item position. [0, cnt) .
// offset is key_value offset from first byte of block.
func getOffsetSizeFromBlock(block []byte, n int) (offset uint16, size uint32) {

	off := lv1BlockCntSize + lv1BlockMaxItems*8 // offset_size pairs start from off.

	os := block[off+n*6 : off+n*6+6]

	return binary.LittleEndian.Uint16(os[:2]), binary.LittleEndian.Uint32(os[2:])
}
