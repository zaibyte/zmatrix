package zmperf

import (
	"encoding/binary"
	"errors"
	"log"
	"math/rand"
	"time"

	"g.tesamc.com/IT/zaipkg/orpc"

	"github.com/templexxx/tsc"
)

var testVal []byte

func randFillVal(n int64) {
	rand.Seed(tsc.UnixNano())

	testVal = make([]byte, n)
	rand.Read(testVal)
}

// prepareRead using batch set for speeding up.
func (r *Runner) prepareRead() (setCost int64, cntTooManyRequest int) {

	MBs := r.cfg.MBPerGetThread
	cntInThread := MBs * 1024 * 1024 / int(r.cfg.ValSize)

	r.keyMax = uint32(cntInThread)

	eachBatch := 512 * 1024 / int(r.cfg.ValSize)

	k := cntInThread / eachBatch
	r.keyMax = uint32(eachBatch * k)

	log.Printf("prepare with: %d items\n", r.keyMax+1)

	keys := make([][]byte, eachBatch)
	for i := range keys {
		keys[i] = make([]byte, 8)
	}
	vals := make([][]byte, eachBatch)
	for i := range vals {
		vals[i] = make([]byte, int(r.cfg.ValSize))
		copy(vals[i], testVal)
	}

	start := time.Now().UnixNano()
	key := uint32(0)
	for j := 0; j < k; j++ {
		for i := range keys {
			binary.BigEndian.PutUint64(keys[i], uint64(key))
			key++
		}

		for {
			err := r.client.SetBatch(0, keys, vals)
			if err != nil {
				if errors.Is(err, orpc.ErrTooManyRequests) {
					time.Sleep(3 * time.Second) // Sleep for a while for transferring.
					err = nil
					cntTooManyRequest++
					continue
				} else {
					log.Fatal("prepare items failed: ", err)
				}
			}

			if err == nil {
				break
			}
		}
	}
	setCost = time.Now().UnixNano() - start

	time.Sleep(10 * time.Second) // Waiting for potential transferring before seal.

	err := r.client.Seal(0)
	if err != nil {
		log.Fatal("seal database failed: ", err)
	}

	time.Sleep(10 * time.Second) // Waiting for potential transferring done.

	// Restart client is useful when it's zMatrix Server, because sealed neo database will start pebbleDB in read-only model,
	// in this model, the compaction will be stopped. And lv1 could get full ability of I/O.
	r.client.Stop(nil)

	err = r.client.Start()
	if err != nil {
		log.Fatal("restart client failed: ", err)
	}

	return
}
