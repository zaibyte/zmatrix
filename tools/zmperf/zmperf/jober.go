package zmperf

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/xmath/xrand"

	"g.tesamc.com/IT/zmatrix/pkg/xrpc"

	"g.tesamc.com/IT/zaipkg/xtest"

	"github.com/templexxx/tsc"
)

// jober is container of perf job, each thread has one.
// jober will count the index of Extenter list,
// both of get  will have their own Extenter list for being used independently.
type jober struct {
	client xrpc.Client

	buf         []byte
	isDoNothing bool
	ignoreError bool

	oids []uint64
}

func newJober(client xrpc.Client, valSize int64, isDoNothing, ignoreError bool) *jober {

	return &jober{
		client:      client,
		buf:         make([]byte, valSize),
		isDoNothing: isDoNothing,
		ignoreError: ignoreError,
	}
}

func (j *jober) get(key []byte) (bool, int64) {

	if j.isDoNothing {
		start := tsc.UnixNano()
		xtest.DoNothing(10)
		cost := tsc.UnixNano() - start
		return true, cost
	}

	start := tsc.UnixNano()
	_, closer, err := j.client.Get(0, key)
	cost := tsc.UnixNano() - start
	if err != nil {
		if errors.Is(err, orpc.ErrNotFound) {
			if !j.ignoreError {
				xlog.Fatalf("failed to get, not found for key: %d", binary.BigEndian.Uint64(key))
			}
		}
		return false, cost
	}
	defer closer.Close()
	return true, cost
}

func (r *Runner) runGetJob(wg *sync.WaitGroup) {

	jobers := r.getJobers

	for _, j := range jobers {
		go func(jober *jober) {
			defer wg.Done()

			key := make([]byte, 8)

			for k := 0; k < int(r.keyMax); k++ {

				binary.BigEndian.PutUint64(key, uint64(xrand.Uint32n(r.keyMax)))

				ok, cost := jober.get(key)

				now := tsc.UnixNano()

				if now >= r.stopTS {
					return
				}

				if ok {
					delta := now - r.startTS
					if delta > r.cfg.SkipTime {
						_ = r.getLat.RecordValuesAtomic(cost, 1)
					}

					atomic.AddInt64(&r.getOK, 1)
				} else {
					atomic.AddInt64(&r.getFailed, 1)
				}

			}
		}(j)
	}
}
