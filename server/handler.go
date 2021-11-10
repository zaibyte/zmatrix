package server

import (
	"errors"
	"io"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xerrors"
	config2 "g.tesamc.com/IT/zmatrix/pkg/config"
	"g.tesamc.com/IT/zproto/pkg/zmatrixpb"
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

	d, err := s.mgr.GetDB(db)
	if err != nil {
		if errors.Is(err, orpc.ErrNotFound) {
			dp, err := s.mgr.PickDisk()
			if err != nil {
				return err
			}
			d, err = s.mgr.CreateDB(db, dp, zmatrixpb.DBEngine_DB_Engine_Neo)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return d.Set(key, value)
}

func (s *Server) Get(db uint32, key []byte) (value []byte, closer io.Closer, err error) {

	if db > config2.MaxDBNum {
		return nil, nil, xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
	}

	if len(key) > config2.MaxKeyLen {
		return nil, nil, xerrors.WithMessage(orpc.ErrBadRequest, "too long key")
	}

	d, err := s.mgr.GetDB(db)
	if err != nil {
		return nil, nil, err
	}
	return d.Get(key)
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

	d, err := s.mgr.GetDB(db)
	if err != nil {
		if errors.Is(err, orpc.ErrNotFound) {
			dp, err := s.mgr.PickDisk()
			if err != nil {
				return err
			}
			d, err = s.mgr.CreateDB(db, dp, zmatrixpb.DBEngine_DB_Engine_Neo)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return d.SetBatch(keys, values)
}

func (s *Server) Remove(db uint32) error {

	if db > config2.MaxDBNum {
		return xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
	}

	return s.mgr.RemoveDB(db)
}

func (s *Server) Seal(db uint32) error {

	if db > config2.MaxDBNum {
		return xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
	}

	d, err := s.mgr.GetDB(db)
	if err != nil {
		return err
	}
	return d.Seal()
}

func (s *Server) GetState(db uint32) (zmatrixpb.DBState, error) {
	if db > config2.MaxDBNum {
		return 0, xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
	}

	d, err := s.mgr.GetDB(db)
	if err != nil {
		return 0, err
	}
	return d.GetState(), nil
}
