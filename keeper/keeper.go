package keeper

import "g.tesamc.com/IT/zmatrix/db"

// Keeper manages all DB.
type Keeper interface {
	// Remove clean up all resource of this DB.
	Remove(dbID uint32) error
	// GetDB gets DB from Keeper.
	GetDB(dbID uint32) (*db.DB, error)

	// PickDisk picks up a disk for new database.
	PickDisk() (diskPath string, err error)
}
