package neo

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zaipkg/xtest"

	"github.com/templexxx/tsc"

	"github.com/cespare/xxhash/v2"
)

func TestBlockSearchPerf(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("prop testing not enabled")
	}

	keyCnt := 8

	keys := make([][]byte, keyCnt)
	for j := range keys {
		keys[j] = make([]byte, 8)
		binary.BigEndian.PutUint64(keys[j], uint64(j))
	}

	pollution1 := make([]byte, 8*1024*1024)
	pollution2 := make([]byte, 8*1024*1024)

	rand.Read(pollution1)

	buf := make([]byte, blockGainSize)
	for j := range keys {
		buf[j*913] = 8
		binary.LittleEndian.PutUint32(buf[j*913+1:j*913+5], 900)
		binary.BigEndian.PutUint64(buf[j*913+5:j*913+13], uint64(j))
	}

	for j := range keys {

		// Let pollution kept in cache.
		// We could use prefetch instruction here, but prefetch is just a hint.
		copy(pollution2, pollution1)
		copy(pollution1, pollution2)
		_ = xxhash.Sum64(pollution1)
		_ = xxhash.Sum64(pollution2)

		start := tsc.UnixNano()
		_, _ = searchInLv1Block(buf, keys[j])
		fmt.Printf("search: %d, cost: %d (ns)", j, tsc.UnixNano()-start)
	}
}

// In blockGainSize buffer's last bytes, only has part/no value.
func TestLv1BlockPartValue(t *testing.T) {

	// Part value in blockGainSize buffer.
	buf := make([]byte, blockGainSize*2)
	key1 := make([]byte, 8)
	val1 := make([]byte, 8000)
	binary.BigEndian.PutUint64(key1, 1)
	n := makeLv1Block(buf, 8, 8000, key1, val1)

	key2 := make([]byte, 8)
	val2 := make([]byte, 8000)
	binary.BigEndian.PutUint64(key2, 2)
	makeLv1Block(buf[n:], 8, 8000, key2, val2)

	offset1, size1 := searchInLv1Block(buf, key1)
	assert.Equal(t, keyLenInBlock+valLenInBlock+8, offset1)
	assert.Equal(t, 8000, size1)

	offset2, size2 := searchInLv1Block(buf, key2)
	assert.Equal(t, n+keyLenInBlock+valLenInBlock+8, offset2)
	assert.Equal(t, 8000, size2)

	// Only key in blockGainSize buffer.
	key2 = make([]byte, blockGainSize-n-keyLenInBlock-valLenInBlock)
	rand.Read(key2)
	makeLv1Block(buf[n:], len(key2), 8000, key2, val2)

	offset1, size1 = searchInLv1Block(buf, key1)
	assert.Equal(t, keyLenInBlock+valLenInBlock+8, offset1)
	assert.Equal(t, 8000, size1)

	offset2, size2 = searchInLv1Block(buf, key2)
	assert.Equal(t, blockGainSize, offset2)
	assert.Equal(t, 8000, size2)
}

func TestLv1BlockRand(t *testing.T) {

	buf := make([]byte, blockGainSize)

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 1; i++ {

		keys := make([][]byte, 0, 1024)
		vals := make([][]byte, 0, 1024)
		offs := make([]int, 0, 1024) // value offsets

		existed := make(map[string]struct{})

		offset := 0
		for {
			kn := rand.Intn(8 + 1)
			vn := rand.Intn(16 + 1)

			if kn == 0 {
				kn = 1
			}
			if vn == 0 {
				vn = 1
			}

			if keyLenInBlock+valLenInBlock+8+16+offset > blockGainSize {
				break
			}

			key := make([]byte, kn)
			val := make([]byte, vn)
			for {
				rand.Read(key)
				if _, ok := existed[string(key)]; ok {
					if kn != 8 {
						key = make([]byte, 8)
						kn = 8 // Bigger key_len for more chance to get unique key.
					}
					continue
				}
				existed[string(key)] = struct{}{}
				break
			}
			// rand.Read(val), empty val must be jump over in search process, otherwise it's bug.

			keys = append(keys, key)
			vals = append(vals, val)

			n := makeLv1Block(buf[offset:], kn, vn, key, val)

			offs = append(offs, offset+keyLenInBlock+valLenInBlock+kn)

			offset += n
		}

		for j, key := range keys {
			off, vs := searchInLv1Block(buf, key)
			assert.Equal(t, offs[j], off)
			assert.Equal(t, len(vals[j]), vs)
		}
	}
}
