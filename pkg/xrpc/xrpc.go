package xrpc

import (
	"io"

	"g.tesamc.com/IT/zmatrix/db"
)

// Client is the xRPC client.
type Client interface {
	db.KVer
	StartStopper
}

type Server interface {
	StartStopper
}

// StartStopper is the xRPC server.
type StartStopper interface {
	Start() error
	// Stop closes instance with an error which will be passed to the pending requests.
	Stop(err error)
}

// ServerHandler is the xRPC handler.
type ServerHandler interface {
	// Set key, value to certain db.
	// If db not found, will be created automatically.
	Set(db uint32, key, value []byte) error
	Get(db uint32, key []byte) (value []byte, closer io.Closer, err error)
	SetBatch(db uint32, keys, values [][]byte) error
}
