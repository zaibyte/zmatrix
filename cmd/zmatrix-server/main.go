package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"github.com/templexxx/tsc"
	"github.com/zaibyte/zaipkg/app"
	"github.com/zaibyte/zaipkg/config"
	"github.com/zaibyte/zaipkg/xbytes"
	"github.com/zaibyte/zaipkg/xerrors"
	"github.com/zaibyte/zaipkg/xlog"
	"github.com/zaibyte/zaipkg/xtime/hlc"
	"github.com/zaibyte/zaipkg/xtime/hlc/mhlc"
	"github.com/zaibyte/zaipkg/xtime/systimemon"
	"github.com/zaibyte/zmatrix/server"
	scfg "github.com/zaibyte/zmatrix/server/config"
)

const _appName = "zmatrix"

func main() {

	config.Init(_appName)

	var cfg scfg.Config
	config.Load(&cfg)

	cfg.App.Adjust()

	_, err := cfg.App.Log.MakeLogger(_appName)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		beforeExit()
	}()

	if cfg.Development {
		xbytes.EnableDefault()
	} else {
		xbytes.EnableMax()
	}

	ctx, cancel := context.WithCancel(context.Background())

	svr, err := server.Create(ctx, &cfg)
	if err != nil {
		xlog.Fatal(xerrors.WithMessage(err, "create server failed").Error())
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	rand.Seed(tsc.UnixNano())

	go systimemon.StartMonitor(ctx, tsc.UnixNano, func() { // HLC clock doesn't like backward.
		xlog.Error("system time jumps backward")
	})

	go app.TimeCalibrateLoop(ctx, cfg.App.TimeCalibrateInterval.Duration)

	mh := mhlc.New()
	hlc.InitGlobalHLC(mh)

	if err = svr.Run(); err != nil {
		svr.Close()
		xlog.Fatal(xerrors.WithMessage(err, "run server failed").Error())
	}

	<-ctx.Done()
	xlog.Infof("got signal to exit: %s", sig.String())

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		beforeExit()
		os.Exit(0)
	default:
		beforeExit()
		os.Exit(1)
	}
}

func beforeExit() {
	_ = xlog.Sync()
	_ = xlog.Close()
}
