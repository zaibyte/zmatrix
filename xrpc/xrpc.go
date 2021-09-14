package xrpc

import (
	"g.tesamc.com/IT/zmatrix/db"
)

// Client is the xRPC client.
type Client interface {
	db.KVer
	StartStoper
}

type Server interface {
	StartStoper
}

// StartStoper is the xRPC server.
type StartStoper interface {
	Start() error
	// Stop closes instance with an error which will be passed to the pending requests.
	Stop(err error)
}

// ServerHandler is the xRPC handler.
type ServerHandler interface {
	Set(db uint32, key, value []byte) error
	Get(db uint32, key []byte) (value []byte, err error)
	SetBatch(db uint32, keys, values [][]byte) error
}
