package neo

import (
	"io"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xerrors"

	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zmatrix/db"
	"g.tesamc.com/IT/zproto/pkg/zmatrixpb"
)

// Neo Layout on local filesystem:
//  .
//  ├── <database_path>
//  │    ├── lv0
//  │    └── lv1
//  │         └── segments
//  │              ├── <segment_id>.seg
//  │              ├── <segment_id>.idx
//	│ 			   ├── ...

const (
	lv0DirName     = "lv0"
	lv1DirName     = "lv1"
	lv1SegsDirName = "segments"
)

type Database struct {
	isRunning int64

	id    uint32
	state int32
	// volatile data, count bytes in lvl0 roughly for triggering flushing job to lvl1.
	// After flushing, should minus bytes flushed.
	lvl0Used       uint64
	lvl0DirtyCount uint64

	fs    vfs.FS
	sched xio.Scheduler
	path  string

	lv0 *lv0
	lv1 *lv1

	isTran int64 // there is lv0 -> lv1 job unfinished.
}

// Create a new neo Database.
func Create(id uint32, path string, fs vfs.FS, sched xio.Scheduler) (*Database, error) {

	d := &Database{
		id:    id,
		state: int32(zmatrixpb.DBState_DB_ReadWrite),
		fs:    fs,
		sched: sched,
		path:  path,
	}

	var err error

	d.lv0, err = createLv0(path, fs)
	if err != nil {
		return nil, err
	}
	d.lv1, err = createLv1(path, fs, sched)
	if err != nil {
		return nil, err
	}

	return d, nil
}

// Load existed neo Database from disk.
func Load(id uint32, path string, fs vfs.FS, sched xio.Scheduler) (*Database, error) {

	d := &Database{
		id:    id,
		state: int32(zmatrixpb.DBState_DB_ReadWrite),
		fs:    fs,
		sched: sched,
		path:  path,
	}

	var err error
	d.lv0, err = loadLv0(path, fs)
	if err != nil {
		return nil, err
	}
	d.lv1, err = loadLv1(path, fs, sched)
	if err != nil {
		_ = d.lv0.close()
		return nil, err
	}

	return d, nil
}

func (d *Database) Start() error {

	atomic.StoreInt64(&d.isRunning, 1)
	return nil
}

func (d *Database) GetState() zmatrixpb.DBState {

	state := atomic.LoadInt32(&d.state)
	return zmatrixpb.DBState(state)
}

func (d *Database) SetState(s zmatrixpb.DBState) (state zmatrixpb.DBState, ok bool) {

	old := atomic.LoadInt32(&d.state)
	if int32(s) == old {
		return s, true
	}
	ok = atomic.CompareAndSwapInt32(&d.state, old, int32(s))
	if ok {
		return s, true
	}
	return d.GetState(), false
}

func (d *Database) Remove() error {

	err := d.Close()
	if err != nil {
		return err
	}

	d.SetState(zmatrixpb.DBState_DB_Removed)

	err = d.fs.RemoveAll(d.path)
	if err != nil {
		return err
	}

	return nil
}

var _ db.DB = new(Database)

func (d *Database) GetID() uint32 {
	return d.id
}

func (d *Database) Set(key, value []byte) error {

	if d.isClosed() {
		return orpc.ErrServiceClosed
	}

	d.doTrans() // Check trans first.

	var err error
	defer func() {
		if err == nil {

			atomic.AddUint64(&d.lvl0Used, uint64(len(key)))
			atomic.AddUint64(&d.lvl0Used, uint64(len(value)))
			atomic.AddUint64(&d.lvl0DirtyCount, 1)
		}
	}()

	// TODO if ErrLv1SegmentsFull, set Database Sealed
	panic("implement me")
}

func (d *Database) Get(key []byte) ([]byte, io.Closer, error) {
	panic("implement me")
}

func (d *Database) SetBatch(keys, values [][]byte) error {
	d.doTrans()

	var err error

	defer func() {
		if err == nil {
			for i := range keys {
				key := keys[i]
				value := values[i]
				atomic.AddUint64(&d.lvl0Used, uint64(len(key)))
				atomic.AddUint64(&d.lvl0Used, uint64(len(value)))
				atomic.AddUint64(&d.lvl0DirtyCount, 1)
			}
		}
	}()
	panic("implement me")
}

func (d *Database) needTrans() bool {

	if atomic.LoadUint64(&d.lvl0Used) > DefaultToLv1Threshold ||
		atomic.LoadUint64(&d.lvl0DirtyCount) > DefaultToLv1MaxEntries {
		return true
	}
	return false
}

// 1. check threshold
// 2. if matched, check if there is job already
// 3. if no, run tran in new goroutine
// 4. if yes, do nothing
func (d *Database) doTrans() {
	// 1. write down seg
	// 2. making lv1 index
	// 3. delete one by one in lv0
	// 4. write down lv1 index
	// 5. update index in lv1

	// _, _, err := d.lv1.makeSegFile()
	// if err != nil {
	// 	if errors.Is(err, zmerrors.ErrDatabaseFull) {
	// 		xlog.Warnf("neo trans exited: %s", err.Error())
	// 		err = nil
	// 		d.SetState(zmatrixpb.DBState_DB_Sealed)
	// 		return
	// 	}
	// }
	// if err != nil {
	//
	// }
}

func (d *Database) Seal() error {
	panic("implement me")
}

func (d *Database) Migrate(dst *db.KVer) error {
	return xerrors.WithMessage(orpc.ErrNotImplemented, "neo hasn't implemented Migrate yet")
}

func (d *Database) Close() error {
	if d.isClosed() {
		return nil
	}

	err := d.lv0.close()
	if err != nil {
		xlog.Warnf("failed to close database(neo): %d lv0: %s", d.id, err.Error())
	}

	d.lv1.close()

	atomic.StoreInt64(&d.isRunning, 0)

	xlog.Infof("database(neo): %d is closed", d.id)
	return nil
}

func (d *Database) transUndone() bool {
	return atomic.LoadInt64(&d.isTran) == 1
}

func (d *Database) isClosed() bool {
	return atomic.LoadInt64(&d.isRunning) == 0
}

var _ db.DB = new(Database)
