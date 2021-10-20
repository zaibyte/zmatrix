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
	lvl0Used       uint64
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
	panic("implement me")
}

func (d *Database) Get(key []byte) ([]byte, io.Closer, error) {
	panic("implement me")
}

func (d *Database) SetBatch(keys, values [][]byte) error {
	panic("implement me")
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
