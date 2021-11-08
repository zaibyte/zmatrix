package server

import (
	"context"
	"sync"
	"sync/atomic"

	"g.tesamc.com/IT/zmatrix/pkg/xrpc/urpc"

	"g.tesamc.com/IT/zmatrix/mgr"

	"g.tesamc.com/IT/zmatrix/pkg/xrpc"

	"g.tesamc.com/IT/zaipkg/vdisk"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zmatrix/server/config"
)

// Server is the zMatrix server.
// It's the container which holds all interface for outside using.
type Server struct {
	isServing int64

	cfg *config.Config

	instanceID string

	fs    vfs.FS
	vdisk vdisk.Disk

	rpcSvr xrpc.Server // zMatrix server.

	mgr mgr.IMgr

	ctx    context.Context
	cancel func()

	stopWg *sync.WaitGroup
}

// Create creates a ZBuf server.
func Create(ctx context.Context, cfg *config.Config) (*Server, error) {

	cfg.Adjust()

	s := &Server{fs: vfs.GetFS(), vdisk: vdisk.GetDisk()} // Set default FS & Disk at the beginning.
	s.instanceID = cfg.App.InstanceID
	s.cfg = cfg
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.stopWg = new(sync.WaitGroup)

	s.rpcSvr = urpc.NewServer(cfg.ServerAddr, s)

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

	s.zBufDisks.Init(s.fs)
	s.zBufDisks.StartSched()

	// s.listAndLoadExts()
	//
	// err := s.rpcSvr.Start()
	// if err != nil {
	// 	return err
	// }
	// s.httpSvr.Start()
	//
	// s.state = metapb.ZBufState_ZBuf_Up // set up before heartbeat. heartbeat may change state.
	//
	// s.startBgLoops()
	//
	// xlog.Info("server is running")
	//
	// s.sendZBufHeartbeat()
	// s.sendExtsHeartbeat()

	return nil
}

func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

// startBgLoops starts Server background jobs which running in loops.
func (s *Server) startBgLoops() {
	s.stopWg.Add(2)

	go s.zBufDisks.DetectLoop()
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

	// s.rpcSvr.Stop()
	// s.httpSvr.Close()
	// s.zc.Close(nil)
	//
	// s.stopBgLoops()
	//
	// s.exts.Range(func(key, value interface{}) bool {
	// 	ext := value.(extent.Extenter)
	// 	ext.Close()
	// 	return true
	// })

	s.zBufDisks.CloseSched()

	xlog.Info("server is closed")
}
