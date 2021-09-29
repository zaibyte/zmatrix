package keeper

import (
	"context"
	"sync"

	"g.tesamc.com/IT/zaipkg/vdisk"

	"g.tesamc.com/IT/zaipkg/vfs"

	sdisk "g.tesamc.com/IT/zaipkg/vdisk/svr"

	"g.tesamc.com/IT/zmatrix/db"
)

type Keeper struct {
	cfg *Config

	fs    vfs.FS
	vdisk vdisk.Disk

	dbs   *sync.Map
	disks *sdisk.ZBufDisks

	boot *Boot

	wg     *sync.WaitGroup
	ctx    context.Context
	cancel func()
}

var _ IKeeper = new(Keeper)

func NewKeeper(ctx context.Context, wg *sync.WaitGroup, fs vfs.FS, vdsisk vdisk.Disk, cfg *Config) (k *Keeper, err error) {

	k = new(Keeper)
	k.cfg = cfg
	err = k.fs.MkdirAll(cfg.BootPath, 0755)
	if err != nil {
		return nil, err
	}
	k.boot, err = CreateKpBoot(fs, cfg.BootPath)
	if err != nil {
		return nil, err
	}

	k.ctx, k.cancel = context.WithCancel(ctx)

	k.dbs = new(sync.Map)
	k.wg = wg
	k.fs = fs
	k.vdisk = vdsisk

	k.disks = sdisk.NewZBufDisks(k.ctx, k.wg, k.fs, k.vdisk, cfg.InstanceID, cfg.DataRoot, &cfg.Scheduler)

	return k, nil
}

func (k *Keeper) CreateDB(dbID uint32, diskPath string) (*db.DB, error) {
	panic("implement me")
}

func (k *Keeper) RemoveDB(dbID uint32) error {
	panic("implement me")
}

func (k *Keeper) GetDB(dbID uint32) (*db.DB, error) {
	panic("implement me")
}

func (k *Keeper) PickDisk() (diskPath string, err error) {
	panic("implement me")
}

func (k *Keeper) Close() {

}
