package mgr

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"g.tesamc.com/IT/zmatrix/db/engine/neo"

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

type Mgr struct {
	isServing int64

	cfg *Config

	fs    vfs.FS
	vdisk vdisk.Disk

	dbs []*unsafe.Pointer // *DB

	disks *sdisk.ZBufDisks

	wg     *sync.WaitGroup
	ctx    context.Context
	cancel func()
}

type DB struct {
	db  db.DB
	dir string
}

var _ IMgr = new(Mgr)

func (m *Mgr) getDB(id uint32) *DB {

	p := atomic.LoadPointer(m.dbs[id])
	if p == nil {
		return nil
	}

	return (*DB)(p)
}

func (m *Mgr) setDB(id uint32, db *DB) {

	if db == nil {
		atomic.StorePointer(m.dbs[id], nil)
	} else {
		atomic.StorePointer(m.dbs[id], unsafe.Pointer(db))
	}
}

func New(ctx context.Context, fs vfs.FS, vdsisk vdisk.Disk, cfg *Config) (k *Mgr, err error) {

	k = new(Mgr)
	k.cfg = cfg

	k.ctx, k.cancel = context.WithCancel(ctx)

	k.dbs = make([]*unsafe.Pointer, MaxDBNum)
	k.wg = new(sync.WaitGroup)

	k.fs = fs
	k.vdisk = vdsisk
	k.disks = sdisk.NewZBufDisks(k.ctx, k.wg, k.fs, k.vdisk, cfg.InstanceID, cfg.DataRoot, &cfg.Scheduler)

	return k, nil
}

func (m *Mgr) Start() error {

	if !atomic.CompareAndSwapInt64(&m.isServing, 0, 1) {
		// server is already closed
		return nil
	}

	m.disks.Init(m.fs)
	m.disks.StartSched()

	for _, diskID := range m.disks.ListDiskIDs() {
		dp := sdisk.MakeDiskDir(diskID, m.cfg.DataRoot)

		sched, _ := m.disks.GetSched(GetDiskIDFromPath(dp)) // Must be here.

		for i := 0; i < MaxDBNum; i++ {

			dbDir := MakeDBDir(uint32(i), dp)

			_, err := db.LoadBoot(m.fs, dbDir)
			if err != nil {
				err = xerrors.WithMessagef(err, "failed to load database: %d", i)
				xlog.Warn(err.Error())
				continue
			}

			d, err := neo.Load(dbDir, m.fs, sched)
			if err != nil {
				err = xerrors.WithMessagef(err, "failed to load database: %d", i)
				xlog.Warn(err.Error())
				continue
			}
			m.setDB(uint32(i), &DB{
				db:  d,
				dir: dbDir,
			})
		}
	}

	m.startBackgroundLoop()

	xlog.Info("keeper is running")

	return nil
}

func (m *Mgr) startBackgroundLoop() {

	m.wg.Add(1)
	go m.disks.DetectLoopWithUsage()
}

func (m *Mgr) isClosed() bool {
	return atomic.LoadInt64(&m.isServing) == 0
}

// CreateDB creates database on this directory:
// <data_root>/disk_<disk_id>/zmatrix/db_<db_id>, includes:
// 1. create database home directory if not existed: zmatrix/db
// 2. create this database directory
// 3. invoke engine's creating method to create database
// 4. store new db into manager memory
func (m *Mgr) CreateDB(dbID uint32, diskPath string, engine zmatrixpb.DBEngine) (d db.DB, err error) {

	if m.isClosed() {
		return nil, orpc.ErrServiceClosed
	}

	if dbID >= 16 {
		err = xerrors.WithMessage(orpc.ErrBadRequest, fmt.Sprintf("illegal db id, exp < 16; but got: %d", dbID))
		xlog.Error(err.Error())
		return nil, err
	}

	if engine != zmatrixpb.DBEngine_DB_Engine_Neo {
		err = xerrors.WithMessage(orpc.ErrBadRequest, "unsupported db engine")
		xlog.Error(err.Error())
		return nil, err
	}

	sched, ok := m.disks.GetSched(GetDiskIDFromPath(diskPath))
	if !ok {
		err = xerrors.WithMessage(orpc.ErrInternalServer, fmt.Sprintf("failed to find io scheudler for disk: %s",
			diskPath))
		xlog.Error(err.Error())
		return nil, xerrors.WithMessage(orpc.ErrInternalServer, err.Error())
	}

	d, err = m.GetDB(dbID)
	if err == nil {
		return
	}

	dbDir := MakeDBDir(dbID, diskPath)

	fs := m.fs

	defer func() {
		if err != nil {
			_ = fs.RemoveAll(dbDir) // Avoiding garbage.
		}
	}()

	err = fs.MkdirAll(dbDir, 0755)
	if err != nil {
		return nil, err
	}

	d, err = neo.Create(dbDir, fs, sched)
	if err != nil {
		return nil, err
	}

	m.setDB(dbID, &DB{
		db:  d,
		dir: dbDir,
	})

	return
}

// RemoveDB removes database:
// 1. closing database
// 2. mark removed by changed state in boot
// 3. delete directory
//
// 1 & 2 will be done in DB.Close
func (m *Mgr) RemoveDB(dbID uint32) error {

	if m.isClosed() {
		return orpc.ErrServiceClosed
	}

	d := m.getDB(dbID)
	if d == nil {
		return orpc.ErrNotFound
	}
	err := d.db.Remove()
	if err != nil {
		return err
	}

	err = m.fs.RemoveAll(d.dir)
	if err != nil {
		xlog.Error(xerrors.WithMessage(err, "failed to remove database directory").Error())
	}

	return nil
}

func (m *Mgr) GetDB(dbID uint32) (db.DB, error) {

	if m.isClosed() {
		return nil, orpc.ErrServiceClosed
	}

	d := m.getDB(dbID)
	if d == nil {
		return nil, orpc.ErrNotFound
	}
	return d.db, nil
}

// PickDisk picks disk which satisfy min disk space request and has the most free space.
func (m *Mgr) PickDisk() (diskPath string, err error) {

	if m.isClosed() {
		return "", orpc.ErrServiceClosed
	}

	dm := m.disks.CloneAllDiskMeta()
	disks := make([]*metapb.Disk, 0, len(dm))
	for _, m := range dm {
		disks = append(disks, m)
	}

	selected := filter.DiskPool.Get().([]*metapb.Disk)
	defer filter.DiskPool.Put(selected[:0])

	fs := []filter.Filter{
		filter.NewDiskThresholdFilter(uint64(m.cfg.MinDiskSpace)),
	}

	disk := filter.NewCandidates(disks).
		FilterTarget(selected, fs...).
		PickMaxScore()

	if disk == nil {
		return "", xerrors.WithMessage(orpc.ErrNotFound, "no disk found for making new database")
	}

	return sdisk.MakeDiskDir(disk.Id, m.cfg.DataRoot), nil
}

func (m *Mgr) Close() {

	if !atomic.CompareAndSwapInt64(&m.isServing, 1, 0) {
		// keeper is already closed
		return
	}

	xlog.Info("closing keeper")

	m.stopBgLoops()

	for i := 0; i < MaxDBNum; i++ {
		d := m.getDB(uint32(i))
		if d != nil {
			_ = d.db.Close()
		}
	}

	m.disks.CloseSched()

	xlog.Info("keeper is closed")
}

func (m *Mgr) stopBgLoops() {
	m.cancel()
	m.wg.Wait()
}
