package db

// Keeper manages all DB.
type Keeper interface {
	// Remove clean up all resource of this DB.
	Remove(dbID uint32) error
	// GetDB gets DB from Keeper.
	GetDB(dbID uint32) (*DB, error)
}
