package db

import (
	"io"

	"g.tesamc.com/IT/zproto/pkg/zmatrixpb"
)

// DB provides a concurrent, persistent key/value store.
type DB interface {
	Start() error

	// GetID gets DB's id.
	GetID() uint32

	GetState() zmatrixpb.DBState
	SetState(s zmatrixpb.DBState)

	KVer

	// Seal seals DB, rejecting any Set.
	Seal() error

	// Remove marks this database could be removed.
	Remove() error

	// Migrate migrates a sealed DB to another KVer.
	Migrate(dst *KVer) error

	// Close closes database, release runtime resource.
	Close() error
}

type KVer interface {
	// Set sets the value for the given key.
	// Ensure the key in unique, zMatrix won't give any promise about the behavior of overwriting.
	// len(key) must < 64 KiB.
	//
	// It is safe to modify the contents of the arguments after Set returns.
	//
	// If return orpc.ErrDiskFull, please make a new database, it will create a new database on a new disk with free space if has.
	Set(key, value []byte) error
	// Get gets the value for the given key. It returns orpc.ErrNotFound if the DB does
	// not contain the key.
	//
	// The caller should not modify the contents of the returned slice, but it is
	// safe to modify the contents of the argument after Get returns. The returned
	// slice will remain valid until the returned Closer is closed. On success, the
	// caller MUST call closer.Close() or a memory leak will occur.
	Get(key []byte) ([]byte, io.Closer, error)

	// SetBatch sets multi kv pairs in single call for getting more chance to use sequential I/O.
	// The best total length of kv pairs is around 256 - 512 KiB.
	// It is safe to modify the contents of the arguments after SetBatch returns.
	SetBatch(keys, values [][]byte) error
}
