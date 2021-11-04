package neo

import (
	"errors"
	"io"
	"sync/atomic"

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
	id    uint32
	state int32
	// volatile data, count bytes in lvl0 roughly for triggering flushing job to lvl1.
	// After flushing, should minus bytes flushed.
	lvl0Used       uint64
	lvl0DirtyCount uint64
}

func (d *Database) Start() error {
	panic("implement me")
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
	panic("implement me")
}

var _ db.DB = new(Database)

// Create neo Database.
func Create(path string, fs vfs.FS, sched xio.Scheduler) (*Database, error) {
	return nil, nil
}

// createPaths creates paths needed by Neo Database under dir.
// Return nil if all paths created.
func createPaths(dir string, id uint32) error {
	return nil
}

func Load(path string, fs vfs.FS, sched xio.Scheduler) (*Database, error) {
	return nil, nil
}

func (d *Database) GetID() uint32 {
	return d.id
}

var (
	ErrLv1SegmentsFull = errors.New("lv1 reached 1024 segments")
)

func (d *Database) Set(key, value []byte) error {
	d.doTrans()

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
}

func (d *Database) Seal() error {
	panic("implement me")
}

func (d *Database) Migrate(dst *db.KVer) error {
	return xerrors.WithMessage(orpc.ErrNotImplemented, "neo hasn't implemented Migrate yet")
}

func (d *Database) Close() error {
	panic("implement me")
}

var _ db.DB = new(Database)
