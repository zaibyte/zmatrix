package zmperf

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zaibyte/zaipkg/uid"

	"github.com/zaibyte/zaipkg/app"
	"github.com/zaibyte/zmatrix/mgr"
	"github.com/zaibyte/zmatrix/pkg/xrpc/urpc"
	"github.com/zaibyte/zmatrix/server"
	"github.com/zaibyte/zmatrix/server/config"

	"github.com/zaibyte/zmatrix/pkg/xrpc"

	"github.com/elastic/go-hdrhistogram"
	"github.com/zaibyte/zaipkg/xio/sched"

	"github.com/templexxx/tsc"
)

type Runner struct {
	cfg *Config

	startTS int64
	stopTS  int64

	getLat *hdrhistogram.Histogram

	getJobers []*jober

	getOK     int64
	getFailed int64

	client xrpc.Client

	keyMax uint32 // key is start from 0, uint32 is enough for signle database.

	ctx    context.Context
	cancel func()
	stopWg *sync.WaitGroup
}

func Create(ctx context.Context, cfg *Config) (*Runner, error) {

	cfg.adjust()

	r := &Runner{}
	r.cfg = cfg
	r.stopWg = new(sync.WaitGroup)
	r.ctx, r.cancel = context.WithCancel(ctx)

	schedCfg := &sched.Config{
		Threads:     r.cfg.IOThreads,
		QueueConfig: new(sched.QueueConfig),
	}
	if cfg.NopSched {
		schedCfg = nil
	}

	r.cfg.jobType = jobTypes[r.cfg.JobType]

	insID := uid.GenRandInstanceID()
	var err error
	if cfg.jobType == RPC {
		r.client = urpc.NewClient(cfg.ServerAddr)
	} else {
		r.client, err = server.Create(ctx, &config.Config{
			App:        app.Config{InstanceID: insID},
			ServerAddr: "",
			Manager: mgr.Config{
				InstanceID: insID,
				DataRoot:   cfg.DataRoot,
				Scheduler:  schedCfg,
			},
			Embed:       true,
			Development: false,
		})
		if err != nil {
			return nil, err
		}
	}

	if cfg.ValSize == 0 {
		cfg.ValSize = DefaultValSize
	}

	r.cfg.SkipTime = r.cfg.SkipTime * int64(time.Second)
	r.cfg.JobTime = r.cfg.JobTime * int64(time.Second)

	r.getLat = hdrhistogram.New(100, time.Second.Nanoseconds(), 3)

	return r, nil
}

func (r *Runner) Run() (err error) {

	err = r.client.Start()
	if err != nil {
		return err
	}

	r.getJobers = make([]*jober, r.cfg.GetThreads)
	for i := range r.getJobers {
		r.getJobers[i] = newJober(r.client, int64(r.cfg.ValSize), r.cfg.IsDoNothing, r.cfg.IgnoreError)
	}

	randFillVal(int64(r.cfg.ValSize))

	var readCost int64

	start := tsc.UnixNano()
	atomic.StoreInt64(&r.startTS, start)
	atomic.StoreInt64(&r.stopTS, start+r.cfg.JobTime)

	log.Println("start to prepare read")
	prepareStart := tsc.UnixNano()
	setCost, cntTooManyReq := r.prepareRead()
	prepareCost := tsc.UnixNano() - prepareStart
	setCostSec := float64(setCost) / float64(time.Second)
	prepareCnt := r.keyMax + 1

	log.Printf("prepare read done with batch set (512KB each batch), cost: %.2fs (cnt_too_many_req: %d, sleep_for_too_many_req: %ds) for %d items (8B key + %dB value), QPS: %.2f\n",
		setCostSec, cntTooManyReq, cntTooManyReq*3, prepareCnt, r.cfg.ValSize, float64(prepareCnt)/setCostSec)

	atomic.AddInt64(&r.stopTS, prepareCost)

	r.stopWg.Add(r.cfg.GetThreads)

	readStart := tsc.UnixNano()
	go r.runGetJob(r.stopWg)
	r.stopWg.Wait()
	cost := tsc.UnixNano() - readStart
	atomic.StoreInt64(&readCost, cost)
	totalCost := tsc.UnixNano() - start
	r.printStat(totalCost, readCost)

	return r.Close()
}

func (r *Runner) Close() (err error) {

	r.cancel()

	r.client.Stop(nil)

	return nil
}
