package neo

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/cespare/xxhash/v2"
)

const (
	lv1BlockCntSize   = 2
	lv1BlockAlignSize = 8 * 1024 // 8 KiB could get the best balance between index memory overhead & small items random access performance.
	lv1FirstBlockSize = lv1BlockAlignSize
)

func makeFirstBlockHeader(buf []byte, cnt int, hs []uint64, offs []uint16, sizes []uint32) {

	off := lv1BlockCntSize
	binary.LittleEndian.PutUint16(buf[:off], uint16(cnt))
	for i := range hs {
		binary.LittleEndian.PutUint64(buf[off:off+8], hs[i])
		off += 8
	}
	for i := range offs {
		binary.LittleEndian.PutUint16(buf[off:off+2], offs[i])
		off += 2
		binary.LittleEndian.PutUint32(buf[off:off+4], sizes[i])
		off += 4
	}
}

// searchInBlock searches the firstBlock of a key (returned by index),
// return offset from the first byte of firstBlock, and value size if found (ok will be true).
//
// see README.md for the details of block.
func searchInBlock(firstBlock, key []byte) (offset uint16, size uint32, ok bool) {

	cnt := binary.LittleEndian.Uint16(firstBlock[:lv1BlockCntSize])
	if cnt == 1 {
		offset, size = getOffsetSizeFromBlock(firstBlock, 1, 0)
		offset += uint16(len(key))
		ok = true
		return
	}

	h := xxhash.Sum64(key)

	ns, ok := getNFromBlock(firstBlock, int(cnt), h)
	if !ok {
		return 0, 0, false
	}

	for _, n := range ns {
		offset, size = getOffsetSizeFromBlock(firstBlock, int(cnt), n)
		if bytes.Equal(firstBlock[offset:int(offset)+len(key)], key) {
			return offset + uint16(len(key)), size, true
		}
	}
	return 0, 0, false
}

var intsPool = sync.Pool{New: func() interface{} {
	return make([]int, 0, 2)
}}

func getNFromBlock(firstBlock []byte, cnt int, h uint64) (n []int, ok bool) {

	off := lv1BlockCntSize

	n = intsPool.Get().([]int)[:0]

	for i := 0; i < cnt*8; i += 8 {
		if h == binary.LittleEndian.Uint64(firstBlock[off+i:off+i+8]) {
			n = append(n, i)
		}
	}

	if len(n) == 0 {
		intsPool.Put(n[:0])
		return nil, false
	}

	return n, true
}

// cnt is total items count.
// n is item position. [0, cnt) .
// offset is key_value offset from first byte of firstBlock.
func getOffsetSizeFromBlock(firstBlock []byte, cnt, n int) (offset uint16, size uint32) {

	off := lv1BlockCntSize + cnt*8 // offset_size pairs start from off.

	os := firstBlock[off+n*6 : off+n*6+6]

	return binary.LittleEndian.Uint16(os[:2]), binary.LittleEndian.Uint32(os[2:])
}
