package server

import (
	"context"
	"sync"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/vdisk"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zmatrix/mgr"
	"g.tesamc.com/IT/zmatrix/pkg/xrpc"
	"g.tesamc.com/IT/zmatrix/pkg/xrpc/urpc"
	"g.tesamc.com/IT/zmatrix/server/config"
)

// Server is the zMatrix server.
// It's the container which holds all interface for outside using.
type Server struct {
	isServing int64

	cfg *config.Config

	fs    vfs.FS
	vdisk vdisk.Disk

	rpcSvr xrpc.Server // zMatrix rpc server.

	mgr mgr.IMgr

	ctx    context.Context
	cancel func()

	stopWg *sync.WaitGroup
}

// Create creates a zMatrix server.
func Create(ctx context.Context, cfg *config.Config) (*Server, error) {

	cfg.Adjust()

	s := &Server{fs: vfs.GetFS(), vdisk: vdisk.GetDisk()} // Set default FS & Disk at the beginning.
	s.cfg = cfg
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.stopWg = new(sync.WaitGroup)

	if !s.cfg.Embed {
		s.rpcSvr = urpc.NewServer(cfg.ServerAddr, s)
	}

	var err error
	s.mgr, err = mgr.New(s.ctx, s.fs, s.vdisk, &s.cfg.Manager)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) Run() error {

	if !atomic.CompareAndSwapInt64(&s.isServing, 0, 1) {
		// server is already closed
		return nil
	}

	err := s.mgr.Start()
	if err != nil {
		return err
	}

	if !s.cfg.Embed {
		err = s.rpcSvr.Start()
		if err != nil {
			return err
		}
	}

	s.startBgLoops()

	xlog.Info("server is running")

	return nil
}

func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

// startBgLoops starts Server background jobs which running in loops.
func (s *Server) startBgLoops() {
}

// stopBgLoops stops Server background jobs, blocking until all exited.
func (s *Server) stopBgLoops() {
	s.cancel()
	s.stopWg.Wait()
}

func (s *Server) Close() {

	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	xlog.Info("closing server")

	if !s.cfg.Embed {
		s.rpcSvr.Stop(nil)
	}

	s.mgr.Close()
	s.stopBgLoops()

	xlog.Info("server is closed")
}
