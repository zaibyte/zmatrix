package neo

import (
	"encoding/binary"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
)

// TODO

func TestMakeLv1MinBlock(t *testing.T) {

	buf := make([]byte, lv1BlockAlignSize)
	for i := 1; i <= lv1BlockMaxItems; i++ {
		keys := make([][]byte, i)
		for j := range keys {
			keys[j] = make([]byte, 8)
			binary.BigEndian.PutUint64(keys[j], uint64(j))
		}
		hs := make([]uint64, i)
		for j := range hs {
			hs[j] = xxhash.Sum64(keys[j])
		}
		sizes := make([]uint32, i)
		offs := make([]uint16, i)

		off := lv1BlockHeaderSize
		for j := range sizes {
			offs[j] = uint16(off)
			sizes[j] = uint32(1 + j)
			copy(buf[off:], keys[j])
			off += int(sizes[j]) + 8
		}

		makeLv1MinBlock(buf, i, hs, offs, sizes)

		for j := range keys {
			offset, size, ok := searchInBlock(buf, keys[j])
			assert.True(t, ok)
			assert.Equal(t, offs[j]+8, offset) // Should +8, because it returns value's offset.
			assert.Equal(t, sizes[j], size)
		}

		// must not be existed
		if i != 1 { // If i == 1, means the offset returned by index must be the one.
			nokey := make([]byte, 8)
			for k := i + 1; k <= i+1+8; k++ {
				binary.BigEndian.PutUint64(nokey, uint64(k))
				_, _, ok := searchInBlock(buf, nokey)
				assert.False(t, ok)
			}
		}
	}
}
