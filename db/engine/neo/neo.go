package neo

import (
	"io"

	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zmatrix/db"
)

const (
	pebbleName = "pebble"

	toLvl1Threshold = 8 * 1024 * 1024 * 1024 // At least 8 GiB. Around 1 millions objects for small entries.
)

type Database struct {
	id uint32
	// volatile data, count bytes in lvl0 roughly for triggering flushing job to lvl1.
	// After flushing, should minus bytes flushed.
	lvl0Used       uint64
	lvl0DirtyCount uint64
}

// Create neo Database.
func Create(id uint32, path string, fs vfs.FS, sched xio.Scheduler) (*Database, error) {
	return nil, nil
}

func Load() {

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
