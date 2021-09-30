package keeper

import "g.tesamc.com/IT/zmatrix/db"

// IKeeper manages all DB.
type IKeeper interface {
	Start() error

	// CreateDB creates new db.DB.
	CreateDB(dbID uint32, diskPath string) (*db.DB, error)
	// RemoveDB clean up all resource of this DB.
	RemoveDB(dbID uint32) error
	// GetDB gets DB from IKeeper.
	GetDB(dbID uint32) (*db.DB, error)

	// PickDisk picks up a disk for a new database.
	PickDisk() (diskPath string, err error)

	// Close closes IKeeper, release resource in memory.
	Close()
}
