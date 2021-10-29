package mgr

import (
	"path/filepath"
	"strings"
	"sync"

	"g.tesamc.com/IT/zmatrix/db"
	"g.tesamc.com/IT/zproto/pkg/zmatrixpb"
	"github.com/spf13/cast"
)

// IMgr manages all DB.
type IMgr interface {
	Start() error

	// CreateDB creates new db.DB.
	CreateDB(dbID uint32, diskPath string, engine zmatrixpb.DBEngine) (db.DB, error)
	// RemoveDB clean up all resource of this DB.
	RemoveDB(dbID uint32) error
	// GetDB gets DB from IMgr.
	GetDB(dbID uint32) (db.DB, error)

	// PickDisk picks up a disk for a new database.
	PickDisk() (diskPath string, err error)

	// Close closes IMgr, release resource in memory.
	Close()
}

type DBBoot struct {
	sync.RWMutex
	Boot *db.Boot
}

const (
	dbHomeDirName = "zmatrix"
	dbNamePrefix  = "db_"
)

// MakeDBDir makes database path under the diskDir.
func MakeDBDir(dbID uint32, diskDir string) string {
	return filepath.Join(diskDir, dbHomeDirName, dbNamePrefix+cast.ToString(dbID))
}

// IsDBDir returns true if the file name matches database path format and return the dbID.
func IsDBDir(dbDirName string) (bool, uint32) {

	if !strings.HasPrefix(dbDirName, dbNamePrefix) {
		return false, 0
	}

	id, err := cast.ToUint32E(strings.TrimPrefix(dbDirName, dbNamePrefix))
	if err != nil {
		return false, 0
	}

	return true, id
}
