package keeper

import (
	"encoding/binary"
	"math/rand"
	"path/filepath"
	"sync"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zmatrix/pkg/zmerrors"
	"g.tesamc.com/IT/zproto/pkg/zmatrixpb"

	"github.com/templexxx/tsc"
)

const (
	// BootSectorSize is the keeper boot sector's size.
	// The total length of boot is 4 KiB, enough for holding dozens databases,
	// limit the size of boot for atomic updating.
	// (actually device's page cloud be smaller than it, anyway it improves the rate of atomic operation, good news.)
	BootSectorSize = 4 * 1024

	BootSectorName = "kp-boot"
)

type Boot struct {
	sync.RWMutex

	F   vfs.File
	DBs *zmatrixpb.KeeperBoot
}

// There must be only one keeper boot sector for each zMatrix.
var kpBootBuf = xbytes.MakeAlignedBlock(BootSectorSize, 4096)

func init() {
	rand.Seed(tsc.UnixNano())
	rand.Read(kpBootBuf)
}

// CreateKpBoot writes down a data block in a file as the bootstrap of entire zMatrix.
// It contains database_id:database_boot_path mapping.
//
// KpBoot struct:
// 0                                                   BootSectorSize
// | mapping | random_padding | mapping_length(2B) | checksum(4B) |
func CreateKpBoot(fs vfs.FS, bootPath string) (*Boot, error) {

	fp := filepath.Join(bootPath, BootSectorName)
	f, err := fs.Create(fp)
	if err != nil {
		return nil, err
	}

	k := &Boot{
		F: f,
		DBs: &zmatrixpb.KeeperBoot{
			DbBootPaths: make(map[uint32]string),
		},
	}

	err = FlushKpBoot(k)
	if err != nil {
		return nil, err
	}
	return k, nil
}

// FlushKpBoot flushes keeper boot sector to disk.
// Warn:
// Ensure it has been protected by lock before using.
func FlushKpBoot(k *Boot) error {

	n, err := k.DBs.MarshalTo(kpBootBuf)
	if err != nil {
		return err
	}
	if n > BootSectorSize-6 {
		xlog.Errorf("keeper boot sector is out of 4KB: %s", zmerrors.ErrTooManyDatabase.Error())
		return zmerrors.ErrTooManyDatabase
	}

	binary.LittleEndian.PutUint16(kpBootBuf[BootSectorSize-6:], uint16(n))

	cs := xdigest.Sum32(kpBootBuf[:BootSectorSize-4])
	binary.LittleEndian.PutUint32(kpBootBuf[BootSectorSize-4:], cs)

	_, err = k.F.Write(kpBootBuf)

	return err
}

// LoadKpBoot loads keeper boot-sector file, returns *Boot
// Before return, it'll check the checksum.
func LoadKpBoot(fs vfs.FS, bootPath string) (kb *Boot, err error) {

	fp := filepath.Join(bootPath, BootSectorName)
	f, err := fs.Open(fp)
	if err != nil {
		return nil, err
	}

	n, err := f.ReadAt(kpBootBuf, 0)
	if err != nil {
		return nil, err
	}
	if n != BootSectorSize {
		xlog.Errorf("load keeper sector failed: mismatched size, exp: %d, got: %d", BootSectorSize, n)
		return nil, orpc.ErrInternalServer
	}

	csExp := binary.LittleEndian.Uint32(kpBootBuf[BootSectorSize-4:])
	csAct := xdigest.Sum32(kpBootBuf[:BootSectorSize-4])

	if csExp != csAct {
		return nil, xerrors.WithMessage(orpc.ErrChecksumMismatch, "kp-boot-sector checksum mismatch")
	}

	mn := binary.LittleEndian.Uint16(kpBootBuf[BootSectorSize-6:])

	kb = &Boot{
		F: f,
		DBs: &zmatrixpb.KeeperBoot{
			DbBootPaths: make(map[uint32]string),
		},
	}
	err = kb.DBs.Unmarshal(kpBootBuf[:mn])
	if err != nil {
		return nil, err
	}

	return kb, nil
}

func (k *Boot) Close() {
	_ = k.F.Close()
}

func (k *Boot) Add(id uint32, dbPath string) error {
	k.Lock()
	defer k.Unlock()

	k.DBs.DbBootPaths[id] = dbPath

	return FlushKpBoot(k)
}

func (k *Boot) Del(id uint32) error {
	k.Lock()
	defer k.Unlock()

	delete(k.DBs.DbBootPaths, id)

	return FlushKpBoot(k)
}

func (k *Boot) Search(id uint32) (string, error) {
	k.RLock()
	defer k.RUnlock()

	p, ok := k.DBs.DbBootPaths[id]
	if !ok {
		return "", orpc.ErrNotFound
	}
	return p, nil
}
