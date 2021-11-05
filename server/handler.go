package server

import (
	"io"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xerrors"
	config2 "g.tesamc.com/IT/zmatrix/pkg/config"
)

func (s *Server) Set(db uint32, key, value []byte) error {

	if db > config2.MaxDBNum {
		return xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
	}

	if len(key) > config2.MaxKeyLen {
		return xerrors.WithMessage(orpc.ErrBadRequest, "too long key")
	}

	if len(value) > config2.MaxValueLen {
		return xerrors.WithMessage(orpc.ErrBadRequest, "too long value")
	}

	panic("implement me")
}

func (s *Server) Get(db uint32, key []byte) (value []byte, closer io.Closer, err error) {

	if db > config2.MaxDBNum {
		return nil, nil, xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
	}

	if len(key) > config2.MaxKeyLen {
		return nil, nil, xerrors.WithMessage(orpc.ErrBadRequest, "too long key")
	}

	panic("implement me")
}

func (s *Server) SetBatch(db uint32, keys, values [][]byte) error {

	if db > config2.MaxDBNum {
		return xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
	}

	for i, value := range values {
		if len(keys[i]) > config2.MaxKeyLen {
			return xerrors.WithMessage(orpc.ErrBadRequest, "too long key")
		}
		if len(value) > config2.MaxValueLen {
			return xerrors.WithMessage(orpc.ErrBadRequest, "too long value")
		}
	}

	panic("implement me")
}

func (s *Server) Remove(db uint32) error {

	if db > config2.MaxDBNum {
		return xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
	}

	panic("implement me")
}

func (s *Server) Seal(db uint32) error {

	if db > config2.MaxDBNum {
		return xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
	}

	panic("implement me")
}
