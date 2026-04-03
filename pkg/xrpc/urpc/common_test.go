package urpc

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/zaibyte/zaipkg/xmath/xrand"

	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

func TestSetBatchReqCompactExtra(t *testing.T) {

	rand.Seed(tsc.UnixNano())

	cnt := 1024
	keys := make([][]byte, cnt)
	values := make([][]byte, cnt)

	for i := 0; i < cnt; i++ {
		kl := xrand.Uint32n(100)
		if kl == 0 {
			kl = 1
		}
		key := make([]byte, kl)
		rand.Read(key)

		vl := xrand.Uint32n(200)
		if vl == 0 {
			vl = 2
		}
		value := make([]byte, vl)
		rand.Read(value)

		keys[i] = key
		values[i] = value
	}

	v, closer := compactSetBatchReq(keys, values)
	defer closer.Close()

	akeys, avalues := extraSetBatchReq(v)
	for i := 0; i < cnt; i++ {
		assert.True(t, bytes.Equal(keys[i], akeys[i]))
		assert.True(t, bytes.Equal(values[i], avalues[i]))
	}
}
