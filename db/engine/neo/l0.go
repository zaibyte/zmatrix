package neo

import (
	"io"
	"path/filepath"

	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xlog"

	"github.com/cockroachdb/pebble"
)

const l0DirName = "l0"

const (
	defaultMemTableSize = 256 * 1024 * 1024 // 256 MB. We need a bit bigger mem table.
)

type l0 struct {
	dir string
	fs  vfs.FS // Must be directFS, because API is not 100% competitive with pebble vfs.
	db  *pebble.DB
}

func newL0() *l0 {

}

func (l *l0) create(dbPath string) error {

	fs := l.fs

	dir := makeL0Dir(dbPath)
	err := fs.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	l.dir = dir
	db, err := pebble.Open(dir, &pebble.Options{
		Logger:       xlog.GetGRPCLoggerV2(),
		MemTableSize: defaultMemTableSize,
	})
	if err != nil {
		return err
	}
	l.db = db
	return nil
}

func (l *l0) set(key, value []byte) error {

	return l.db.Set(key, value, nil)
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

func makeL0Dir(dbPath string) string {
	return filepath.Join(dbPath, l0DirName)
}
