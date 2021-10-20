package neo

import (
	"io"

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
	id uint32
	// volatile data, count bytes in lvl0 roughly for triggering flushing job to lvl1.
	// After flushing, should minus bytes flushed.
	lvl0Used       int
	lvl0DirtyCount uint64
}

func (d *Database) Start() error {
	panic("implement me")
}

func (d *Database) GetState() zmatrixpb.DBState {
	panic("implement me")
}

func (d *Database) SetState(s zmatrixpb.DBState) {
	panic("implement me")
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

}

func Load(path string, fs vfs.FS, sched xio.Scheduler) (*Database, error) {

}

func (d *Database) GetID() uint32 {
	return d.id
}

func (d *Database) Set(key, value []byte) error {
	d.doTrans()

	var err error
	defer func() {
		if err == nil {
			d.lvl0Used += len(key)
			d.lvl0Used += len(value)
			d.lvl0DirtyCount++
		}
	}()

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
				d.lvl0Used += len(key)
				d.lvl0Used += len(value)
				d.lvl0DirtyCount++
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

}

func (d *Database) Seal() error {
	panic("implement me")
}

func (d *Database) Migrate(dst *db.KVer) error {
	panic("implement me")
}

func (d *Database) Close() error {
	panic("implement me")
}

var _ db.DB = new(Database)
