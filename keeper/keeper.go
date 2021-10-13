package keeper

import (
	"context"
	"sync"
	"sync/atomic"

	"g.tesamc.com/IT/zproto/pkg/zmatrixpb"

	"g.tesamc.com/IT/keeper/pkg/diskpicker/filter"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/vdisk"
	sdisk "g.tesamc.com/IT/zaipkg/vdisk/svr"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zmatrix/db"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

type Keeper struct {
	isServing int64

	cfg *Config

	fs    vfs.FS
	vdisk vdisk.Disk

	dbs   *sync.Map
	disks *sdisk.ZBufDisks

	wg     *sync.WaitGroup
	ctx    context.Context
	cancel func()
}

var _ IKeeper = new(Keeper)

func NewKeeper(ctx context.Context, fs vfs.FS, vdsisk vdisk.Disk, cfg *Config) (k *Keeper, err error) {

	k = new(Keeper)
	k.cfg = cfg
	err = k.fs.MkdirAll(cfg.BootPath, 0755)
	if err != nil {
		return nil, err
	}

	k.ctx, k.cancel = context.WithCancel(ctx)

	k.dbs = new(sync.Map)
	k.wg = new(sync.WaitGroup)
	k.fs = fs
	k.vdisk = vdsisk

	k.disks = sdisk.NewZBufDisks(k.ctx, k.wg, k.fs, k.vdisk, cfg.InstanceID, cfg.DataRoot, &cfg.Scheduler)

	return k, nil
}

func (k *Keeper) Start() error {

	if !atomic.CompareAndSwapInt64(&k.isServing, 0, 1) {
		// server is already closed
		return nil
	}

	k.disks.Init(k.fs)
	k.disks.StartSched()

	k.startBackgroundLoop()

	xlog.Info("keeper is running")

	return nil
}

func (k *Keeper) startBackgroundLoop() {

	k.wg.Add(1)
	go k.disks.DetectLoopWithUsage()
}

func (k *Keeper) CreateDB(dbID uint32, diskPath string, engine zmatrixpb.DBEngine) (db.DB, error) {
	panic("implement me")
}

func (k *Keeper) RemoveDB(dbID uint32) error {
	panic("implement me")
}

func (k *Keeper) GetDB(dbID uint32) (db.DB, error) {
	panic("implement me")
}

// PickDisk picks disk which satisfy min disk space request and has the most free space.
func (k *Keeper) PickDisk() (diskPath string, err error) {

	dm := k.disks.CloneAllDiskMeta()
	disks := make([]*metapb.Disk, 0, len(dm))
	for _, m := range dm {
		disks = append(disks, m)
	}

	selected := filter.DiskPool.Get().([]*metapb.Disk)
	defer filter.DiskPool.Put(selected[:0])

	fs := []filter.Filter{
		filter.NewDiskThresholdFilter(uint64(k.cfg.MinDiskSpace)),
	}

	disk := filter.NewCandidates(disks).
		FilterTarget(selected, fs...).
		PickMaxScore()

	if disk == nil {
		return "", xerrors.WithMessage(orpc.ErrNotFound, "no disk found for making new database")
	}

	return sdisk.MakeDiskDir(disk.Id, k.cfg.DataRoot), nil
}

func (k *Keeper) Close() {

	if !atomic.CompareAndSwapInt64(&k.isServing, 1, 0) {
		// keeper is already closed
		return
	}

	xlog.Info("closing keeper")

	k.stopBgLoops()

	k.disks.CloseSched()

	xlog.Info("keeper is closed")
}

func (k *Keeper) stopBgLoops() {
	k.cancel()
	k.wg.Wait()
}
