package neo

import "encoding/binary"

const (
	lv1BlockCntSize   = 2
	lv1BlockAlignSize = 8 * 1024
	lv1FirstBlockSize = lv1BlockAlignSize
)

// searchInBlock searches the firstBlock of a key (returned by index),
// return offset from the first byte of firstBlock, and value size if found (ok will be true).
//
// see README.md for the details of block.
func searchInBlock(firstBlock, key []byte) (offset uint16, size uint32, ok bool) {
	if firstBlock[0] == 0 {
	}
}

func getOffsetSizeFromBlock(firstBlock []byte, cnt, n int) (offset uint16, size uint32) {

	off := lv1BlockCntSize + cnt*8 // offset_size pairs start from off.

	os := firstBlock[off+n*6 : off+n*6+6]

	return binary.LittleEndian.Uint16(os[:2]), binary.LittleEndian.Uint32(os[2:])
}
