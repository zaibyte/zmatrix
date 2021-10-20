package neo

import (
	"io"
	"path/filepath"

	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xlog"

	"github.com/cockroachdb/pebble"
)

const (
	defaultMemTableSize = 256 * 1024 * 1024 // 256 MB. We need a bit bigger mem table.
)

type l0 struct {
	dir string
	fs  vfs.FS // Must be directFS, because API is not 100% competitive with pebble vfs.
	db  *pebble.DB
}

func createLv0(dbPath string, fs vfs.FS) (l *l0, err error) {

	l.fs = fs
	dir := makeL0Dir(dbPath)
	err = fs.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	l.dir = dir
	db, err := pebble.Open(dir, &pebble.Options{
		Logger:       xlog.GetGRPCLoggerV2(),
		MemTableSize: defaultMemTableSize,
	})
	if err != nil {
		return nil, err
	}
	l.db = db
	return
}

func (l *l0) set(key, value []byte) error {

	return l.db.Set(key, value, pebble.Sync)
}

func (l *l0) batchSet(keys, values [][]byte) error {
	b := l.db.NewBatch()
	defer b.Close()
	for i := range keys {
		err := b.Set(keys[i], values[i], nil)
		if err != nil {
			return err
		}
	}
	return l.db.Apply(b, pebble.Sync)
}

func (l *l0) getSnapshot() *pebble.Snapshot {

	return l.db.NewSnapshot()
}

func (l *l0) delete(key []byte) {

	_ = l.db.Delete(key, nil)
}

func (l *l0) get(key []byte) ([]byte, io.Closer, error) {
	return l.db.Get(key)
}

func (l *l0) close() error {
	return l.db.Close()
}

func makeL0Dir(dbPath string) string {
	return filepath.Join(dbPath, lv0DirName)
}
