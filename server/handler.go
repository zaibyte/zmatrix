package server

import (
	"errors"
	"fmt"
	"io"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xerrors"
	config2 "g.tesamc.com/IT/zmatrix/pkg/config"
	"g.tesamc.com/IT/zproto/pkg/zmatrixpb"
)

func (s *Server) Set(db uint32, key, value []byte) (err error) {

	defer func() {
		if err != nil {
			xlog.Error(err.Error())
		}
	}()

	if db > config2.MaxDBNum {
		err = xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
		return
	}

	nk, nv := len(key), len(value)

	if nk > config2.MaxKeyLen || nk == 0 {
		err = xerrors.WithMessage(orpc.ErrBadRequest, fmt.Sprintf("illegal key len, exp [1, 256), got: %d", nk))
		return
	}
	if nv > config2.MaxValueLen || nv == 0 {
		err = xerrors.WithMessage(orpc.ErrBadRequest, fmt.Sprintf("illegal value len, exp [1B,4M], got: %d B", nv))
		return
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

	err = d.Set(key, value)
	return
}

func (s *Server) Get(db uint32, key []byte) (value []byte, closer io.Closer, err error) {

	defer func() {
		if err != nil {
			xlog.Error(err.Error())
		}
	}()

	if db > config2.MaxDBNum {
		err = xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
		return
	}

	if len(key) > config2.MaxKeyLen {
		err = xerrors.WithMessage(orpc.ErrBadRequest, "too long key")
		return
	}

	d, err := s.mgr.GetDB(db)
	if err != nil {
		return nil, nil, err
	}
	value, closer, err = d.Get(key)
	return
}

func (s *Server) SetBatch(db uint32, keys, values [][]byte) (err error) {

	defer func() {
		if err != nil {
			xlog.Error(err.Error())
		}
	}()

	if db > config2.MaxDBNum {
		err = xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
		return
	}

	for i := range values {

		nk, nv := len(keys[i]), len(values[i])

		if nk > config2.MaxKeyLen || nk == 0 {
			err = xerrors.WithMessage(orpc.ErrBadRequest, fmt.Sprintf("illegal key len, exp [1, 256), got: %d", nk))
			return
		}
		if nv > config2.MaxValueLen || nv == 0 {
			err = xerrors.WithMessage(orpc.ErrBadRequest, fmt.Sprintf("illegal value len, exp [1B,4M], got: %d B", nv))
			return
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

	err = d.SetBatch(keys, values)
	return
}

func (s *Server) Remove(db uint32) (err error) {

	defer func() {
		if err != nil {
			xlog.Error(err.Error())
		}
	}()

	if db > config2.MaxDBNum {
		err = xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
		return
	}

	err = s.mgr.RemoveDB(db)
	return
}

func (s *Server) Seal(db uint32) (err error) {

	defer func() {
		if err != nil {
			xlog.Error(err.Error())
		}
	}()

	if db > config2.MaxDBNum {
		err = xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
		return
	}

	d, err := s.mgr.GetDB(db)
	if err != nil {
		return err
	}
	err = d.Seal()
	return
}

func (s *Server) GetState(db uint32) (state zmatrixpb.DBState, err error) {

	defer func() {
		if err != nil {
			xlog.Error(err.Error())
		}
	}()

	if db > config2.MaxDBNum {
		err = xerrors.WithMessage(orpc.ErrBadRequest, "too big db id")
		return
	}

	d, err := s.mgr.GetDB(db)
	if err != nil {
		return 0, err
	}
	return d.GetState(), nil
}
