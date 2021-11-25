package zmerrors

import (
	"errors"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xerrors"
)

var (
	ErrTooManyDatabase = errors.New("too many database")
	ErrTooFastSet      = xerrors.WithMessage(orpc.ErrTooManyRequests, "set too fast, pls wait for a while")
	ErrDatabaseFull    = xerrors.WithMessage(orpc.ErrExtentFull, "database is full, pls create a new one")
	ErrDatabaseBroken  = xerrors.WithMessage(orpc.ErrExtentBroken, "database is broken")
)
