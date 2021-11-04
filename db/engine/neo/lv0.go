package neo

import (
	"io"
	"path/filepath"

	"g.tesamc.com/IT/zaipkg/orpc"

	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xlog"

	"github.com/cockroachdb/pebble"
)

const (
	// 256 MB. We need a bit bigger mem table for speeding up lv0.
	// It'll be a good news, if lv0 & lv1 have the same get performance.
	defaultMemTableSize = 256 * 1024 * 1024
)

type lv0 struct {
	dir string
	fs  vfs.FS // Must be directFS, because API is not 100% competitive with pebble vfs.
	db  *pebble.DB
}

func createOrLoadLv0(dbPath string, fs vfs.FS, isCreate bool) (*lv0, error) {

	dir := makeL0Dir(dbPath)
	if isCreate {
		_ = fs.RemoveAll(dir)
		err := fs.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	} else {
		if !vfs.IsDirExisted(fs, dir) {
			return nil, orpc.ErrNotFound
		}
	}

	db, err := pebble.Open(dir, &pebble.Options{
		Logger:       xlog.GetGRPCLoggerV2(),
		MemTableSize: defaultMemTableSize,
	})
	if err != nil {
		return nil, err
	}
	return &lv0{
		dir: dir,
		fs:  fs,
		db:  db,
	}, nil
}

func createLv0(dbPath string, fs vfs.FS) (*lv0, error) {

	return createOrLoadLv0(dbPath, fs, true)
}

func loadLv0(dbPath string, fs vfs.FS) (l *lv0, err error) {

	return createOrLoadLv0(dbPath, fs, false)
}

func (l *lv0) set(key, value []byte) error {

	return l.db.Set(key, value, pebble.Sync)
}

func (l *lv0) batchSet(keys, values [][]byte) error {
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

func (l *lv0) getSnapshot() *pebble.Snapshot {

	return l.db.NewSnapshot()
}

func (l *lv0) delete(key []byte) {

	_ = l.db.Delete(key, nil)
}

func (l *lv0) get(key []byte) ([]byte, io.Closer, error) {
	return l.db.Get(key)
}

func (l *lv0) close() error {
	return l.db.Close()
}

func makeL0Dir(dbPath string) string {
	return filepath.Join(dbPath, lv0DirName)
}
