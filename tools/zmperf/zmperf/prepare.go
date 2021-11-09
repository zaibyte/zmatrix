package zmperf

import (
	"encoding/binary"
	"log"
	"math/rand"

	"github.com/templexxx/tsc"
)

var testVal []byte

func randFillVal(n int64) {
	rand.Seed(tsc.UnixNano())

	testVal = make([]byte, n)
	rand.Read(testVal)
}

// prepareRead using batch set for speeding up.
func (r *Runner) prepareRead() {

	MBs := r.cfg.MBPerGetThread
	cntInThread := MBs * 1024 * 1024 / int(r.cfg.ValSize)

	r.keyMax = uint32(cntInThread)

	eachBatch := 512 * 1024 / int(r.cfg.ValSize)

	k := cntInThread / eachBatch
	r.keyMax = uint32(eachBatch * k)

	keys := make([][]byte, eachBatch)
	for i := range keys {
		keys[i] = make([]byte, 8)
	}
	vals := make([][]byte, eachBatch)
	for i := range vals {
		vals[i] = make([]byte, int(r.cfg.ValSize))
		copy(vals[i], testVal)
	}

	key := uint32(0)
	for j := 0; j < k; k++ {
		for i := range keys {
			binary.BigEndian.PutUint64(keys[i], uint64(key))
			key++
		}
		err := r.client.SetBatch(0, keys, vals)
		if err != nil {
			log.Fatal("prepare objects failed", err)
		}
	}

}
