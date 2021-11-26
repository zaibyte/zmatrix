package neo

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	"g.tesamc.com/IT/zmatrix/pkg/zmerrors"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xerrors"

	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zmatrix/db"
	"g.tesamc.com/IT/zproto/pkg/zmatrixpb"
)

// Neo Layout on local filesystem:
//  .
//  ├── <database_path>
//  │    ├── lv0
//  │    └── lv1
//  │         └── segments
//  │              ├── <segment_id>.seg
//  │              ├── <segment_id>.idx
//	│ 			   ├── ...

const (
	lv0DirName     = "lv0"
	lv1DirName     = "lv1"
	lv1SegsDirName = "segments"
)

type Database struct {
	cfg *Config

	isRunning int64

	id    uint32
	state int32
	// volatile data, count bytes in lvl0 roughly for triggering flushing job to lvl1.
	// After flushing, should minus bytes flushed.
	lv0Used     uint64 // In present, count key+value.
	lv0DirtyCnt uint64

	fs    vfs.FS
	sched xio.Scheduler
	path  string

	lv0 *lv0
	lv1 *lv1

	isTran int64 // there is lv0 -> lv1 job unfinished.
}

// CreateBroken creates broken Database when load failed.
func CreateBroken(id uint32, path string) (*Database, error) {

	d := new(Database)
	d.id = id
	d.path = path
	d.state = int32(zmatrixpb.DBState_DB_Broken)
	return d, nil
}

// Create a new neo Database.
func Create(cfg *Config, id uint32, path string, fs vfs.FS, sched xio.Scheduler) (*Database, error) {

	if cfg == nil {
		cfg = DefaultConfig
	}
	cfg.Adjust()

	d := &Database{
		cfg:   cfg,
		id:    id,
		state: int32(zmatrixpb.DBState_DB_ReadWrite),
		fs:    fs,
		sched: sched,
		path:  path,
	}

	var err error

	d.lv0, err = createLv0(path, fs)
	if err != nil {
		return nil, err
	}
	d.lv1, err = createLv1(path, fs, sched)
	if err != nil {
		return nil, err
	}

	return d, nil
}

// Load existed neo Database from disk.
//
// In Load process, we maybe just failed in last transfer job which means the next transfer job
// may twice bigger than we expect, it's okay.
// lv1 could hold that big segment.
func Load(cfg *Config, id uint32, path string, fs vfs.FS, sched xio.Scheduler, isSealed bool) (*Database, error) {

	if cfg == nil {
		cfg = DefaultConfig
	}

	cfg.Adjust()

	state := int32(zmatrixpb.DBState_DB_ReadWrite)
	if isSealed {
		state = int32(zmatrixpb.DBState_DB_Sealed)
	}

	d := &Database{
		cfg:   cfg,
		id:    id,
		state: state,
		fs:    fs,
		sched: sched,
		path:  path,
	}

	var err error
	d.lv0, err = loadLv0(path, fs, isSealed)
	if err != nil {
		return nil, err
	}

	iter := d.lv0.db.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		d.lv0DirtyCnt++
		d.lv0Used += uint64(len(k) + len(iter.Value()) + keyLenInBlock + valLenInBlock)
	}
	_ = iter.Close()

	if isSealed && d.lv0DirtyCnt == 0 {
		_ = d.lv0.close()
		d.lv0 = nil
		xlog.Infof("database: %d is sealed & lv0 is empty", d.id)
	}

	xlog.Debugf("database: %d load lv0 with items: %d used: %d bytes", d.id, d.lv0DirtyCnt, d.lv0Used)

	d.lv1, err = loadLv1(path, fs, sched)
	if err != nil {
		if d.lv0 != nil {
			_ = d.lv0.close()
		}
		return nil, err
	}

	return d, nil
}

func (d *Database) Start() error {

	if !atomic.CompareAndSwapInt64(&d.isRunning, 0, 1) {
		return nil
	}

	xlog.Infof("database(neo): %d is running", d.id)

	return nil
}

func (d *Database) GetState() zmatrixpb.DBState {

	state := atomic.LoadInt32(&d.state)
	return zmatrixpb.DBState(state)
}

func (d *Database) SetState(s zmatrixpb.DBState) (state zmatrixpb.DBState, ok bool) {

	old := atomic.LoadInt32(&d.state)
	if int32(s) == old {
		return s, true
	}

	if s == zmatrixpb.DBState_DB_ReadWrite { // New read write state could not be executed.
		return zmatrixpb.DBState(old), false
	}

	olds := zmatrixpb.DBState(old)
	if olds == zmatrixpb.DBState_DB_Broken || olds == zmatrixpb.DBState_DB_Removed { // Broken/Removed state cannot be changed.
		return olds, false
	}

	ok = atomic.CompareAndSwapInt32(&d.state, old, int32(s))
	if ok {
		return s, true
	}
	return d.GetState(), false
}

func (d *Database) Remove() error {

	err := d.Close()
	if err != nil {
		return err
	}

	d.SetState(zmatrixpb.DBState_DB_Removed)

	return nil
}

var _ db.DB = new(Database)

func (d *Database) GetID() uint32 {
	return d.id
}

func (d *Database) Set(key, value []byte) error {

	setOK, needTrans, err := d.setCheck()
	if err != nil {
		return err
	}

	if needTrans {
		go d.doTrans(false, false)
	}

	if !setOK { // Must be undone transfer job.
		return zmerrors.ErrTooFastSet
	}

	defer func() {
		if err == nil {

			atomic.AddUint64(&d.lv0Used, uint64(minBlock+len(key)+len(value)))

			atomic.AddUint64(&d.lv0DirtyCnt, 1)
		}
	}()

	err = d.lv0.set(key, value)
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) SetBatch(keys, values [][]byte) error {

	setOK, needTrans, err := d.setCheck()
	if err != nil {
		return err
	}

	if needTrans {
		go d.doTrans(false, false)
	}

	if !setOK { // Must be undone transfer job.
		return zmerrors.ErrTooFastSet
	}

	defer func() {
		if err == nil {
			for i := range keys {
				key := keys[i]
				value := values[i]

				atomic.AddUint64(&d.lv0Used, uint64(minBlock+len(key)+len(value)))
				atomic.AddUint64(&d.lv0DirtyCnt, 1)
			}
		}
	}()

	err = d.lv0.batchSet(keys, values)
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) Get(key []byte) ([]byte, io.Closer, error) {

	err := d.getCheck()
	if err != nil {
		return nil, nil, err
	}

	// search in lv0 first:
	// 1. If key is still in lv0, found. There won't be many keys in lv0, so the performance is acceptable.
	// 2. If key is not found, must be lv1 if existed.

	emptyLv0 := false
	if d.GetState() == zmatrixpb.DBState_DB_Sealed {
		if atomic.LoadUint64(&d.lv0DirtyCnt) == 0 {
			emptyLv0 = true
		}
	}

	if d.lv0 != nil && !emptyLv0 {

		v, closer, err := d.lv0.get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				err = nil // Try to Get in lv1.
			} else {
				return nil, nil, err
			}
		} else {
			return v, closer, nil
		}
	}

	v, closer, err := d.lv1.search(key)
	if err != nil {
		return nil, nil, err
	}

	return v, closer, err

}

func (d *Database) getCheck() (err error) {
	if d.isClosed() {
		return orpc.ErrServiceClosed
	}

	state := d.GetState()
	if state == zmatrixpb.DBState_DB_Sealed || state == zmatrixpb.DBState_DB_ReadWrite {
		return nil
	}
	return xerrors.WithMessage(orpc.ErrInternalServer, fmt.Sprintf("database: %d cannot get caused by state: %s", d.id, state.String()))
}

// setCheck checks states and transfer before set.
func (d *Database) setCheck() (setOK, needTran bool, err error) {

	if d.isClosed() {
		return false, false, orpc.ErrServiceClosed
	}

	state := d.GetState()
	if state != zmatrixpb.DBState_DB_ReadWrite {
		return false, false,
			xerrors.WithMessage(orpc.ErrInternalServer, fmt.Sprintf("database: %d cannot set caused by state: %s", d.id, state.String()))
	}

	setOK = true // We could set at most case unless there is undone transfer job.

	undoneTran := d.transUndone()

	if undoneTran {
		setOK = false
		return
	}

	needTran = d.needTrans()

	return
}

func (d *Database) needTrans() bool {

	if atomic.LoadUint64(&d.lv0Used) >= uint64(d.cfg.ToLv1Threshold) ||
		atomic.LoadUint64(&d.lv0DirtyCnt) >= d.cfg.ToLv1MaxEntries {
		return true
	}
	return false
}

// doTrans in background goroutine
//
// set compact true, if after trans need compact.
// when must is ture, trans must be done. Only set true when seal.
func (d *Database) doTrans(compact, must bool) {

	if must {
		for {
			if atomic.CompareAndSwapInt64(&d.isTran, 0, 1) {
				break
			}
		}
	} else {
		if !atomic.CompareAndSwapInt64(&d.isTran, 0, 1) {
			return
		}
	}

	var dirty, used uint64
	dirty = atomic.LoadUint64(&d.lv0DirtyCnt) // It's not accurate, but not harmful.
	used = atomic.LoadUint64(&d.lv0Used)
	var err error

	if dirty == 0 {
		return
	}

	defer func() {
		if err == nil {
			atomic.AddUint64(&d.lv0Used, ^(used - 1))
			atomic.AddUint64(&d.lv0DirtyCnt, ^(dirty - 1))
			atomic.CompareAndSwapInt64(&d.isTran, 1, 0)
			if must {
				_, _ = d.SetState(zmatrixpb.DBState_DB_Sealed) // must is come with seal.
			}
		} else {
			if errors.Is(err, zmerrors.ErrDatabaseFull) {
				d.SetState(zmatrixpb.DBState_DB_Full)
				xlog.Warnf("database(neo): %d is full: %s", d.id, err.Error())
				err = nil
				atomic.CompareAndSwapInt64(&d.isTran, 1, 0)
				return
			} else {
				xlog.Errorf("database(neo): %d is broken: %s", d.id, err.Error())
				d.SetState(zmatrixpb.DBState_DB_Broken)
				return
			}
		}
	}()

	// 1. write down seg
	// 2. making lv1 index
	// 3. write down lv1 index & update index in lv1 ( done persistIdx)
	// 4. delete one by one in lv0 (dedup in next trans process if crash in deletion)

	snap := d.lv0.getSnapshot()
	segID, idx, min, max, _, err := d.lv1.makeSegIdx(snap, int(dirty), int64(used))
	if err != nil {
		xlog.Errorf("neo: failed to make segment & index: %s", err.Error())
		return
	}

	err = d.lv1.persistIdx(segID, idx, min, max)
	if err != nil {
		xlog.Errorf("neo: failed to persist index: %s", err.Error())
		return
	}

	iter := snap.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		d.lv0.delete(k)
	}
	_ = iter.Close()
	_ = snap.Close()

	if compact {
		_ = d.lv0.db.Compact(min, max)
	}
}

func (d *Database) Seal() error {

	if d.isClosed() {
		return orpc.ErrServiceClosed
	}

	state := d.GetState()
	if state == zmatrixpb.DBState_DB_Sealed {
		return nil
	}
	if state != zmatrixpb.DBState_DB_ReadWrite {
		return xerrors.WithMessage(orpc.ErrBadRequest, "cannot seal database for non-readwrite")
	}

	go d.doTrans(true, true)
	return nil
}

func (d *Database) Migrate(dst *db.KVer) error {
	return xerrors.WithMessage(orpc.ErrNotImplemented, "neo hasn't implemented Migrate yet")
}

func (d *Database) Close() error {
	if !atomic.CompareAndSwapInt64(&d.isRunning, 1, 0) {
		// already closed
		return nil
	}

	for {
		if d.transUndone() {
			time.Sleep(time.Second * 5) // It's the easiest way to wait transfer job done.
		} else {
			break
		}
	}

	if d.lv0 != nil {
		err := d.lv0.close()
		if err != nil {
			xlog.Warnf("failed to close database(neo): %d lv0: %s", d.id, err.Error())
		}
	}

	d.lv1.close()

	xlog.Infof("database(neo): %d is closed", d.id)
	return nil
}

func (d *Database) transUndone() bool {
	return atomic.LoadInt64(&d.isTran) == 1
}

func (d *Database) isClosed() bool {
	return atomic.LoadInt64(&d.isRunning) == 0
}

var _ db.DB = new(Database)
