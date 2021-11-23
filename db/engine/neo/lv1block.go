package neo

import (
	"bytes"
	"encoding/binary"

	"g.tesamc.com/IT/zaipkg/xmath"
	"g.tesamc.com/IT/zmatrix/pkg/config"
)

const (
	keyLenInBlock = 1
	valLenInBlock = 4
	minBlock      = keyLenInBlock + valLenInBlock + config.MinKeyLen + config.MinValueLen
	// 4KB is too small for most NVMe device's nature page size.
	// Every file write will be aligned to this value, and the min offset gap is this.
	// Don't set it too big, because it'll waste lots of I/O when most of the items are small.
	blockGainSize = 8 * 1024
)

var (
	maxBlockSize = xmath.AlignSize(keyLenInBlock+valLenInBlock+config.MaxKeyLen+config.MaxValueLen, blockGainSize)
)

func makeLv1Block(buf []byte, kLen, vLen int, key, value []byte) int {

	off := 0
	buf[0] = uint8(kLen)
	off += keyLenInBlock
	binary.LittleEndian.PutUint32(buf[off:], uint32(vLen))
	off += valLenInBlock

	copy(buf[off:], key)
	off += kLen
	copy(buf[off:], value)

	return off + vLen
}

// searchInLv1Block searches the block of a key (returned by index),
// return the value offset from the first byte of block, and its size if found.
// return -1 if not found.
//
// see README.md for the details of block.
func searchInLv1Block(buf, key []byte) (offset, size int) {

	eklen := len(key)

	headerSize := keyLenInBlock + valLenInBlock

	start := 0
	for start < len(buf) {

		if start+minBlock > len(buf) {
			break
		}

		klen := int(buf[start])

		if klen == 0 {
			break
		}

		vlen := int(binary.LittleEndian.Uint32(buf[start+1:]))
		if klen != eklen {
			start += klen + vlen + headerSize
			continue
		}

		k := buf[start+headerSize : start+headerSize+klen]
		if bytes.Equal(key, k) {
			return start + headerSize + klen, vlen
		}
		start += klen + vlen + headerSize
	}

	return -1, 0
}
