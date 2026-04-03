package db

import (
	"encoding/binary"
	"path/filepath"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/zaibyte/zaipkg/directio"
	"github.com/zaibyte/zaipkg/orpc"
	"github.com/zaibyte/zaipkg/vfs"
	"github.com/zaibyte/zaipkg/xdigest"
	"github.com/zaibyte/zaipkg/xerrors"
	"github.com/zaibyte/zaipkg/xlog"
	"github.com/zaibyte/zproto/pkg/zmatrixpb"
)

// db boot sector is built for starting database correctly:
//
// DBBoot sector struct:
// 0                                                    BootSectorSize
// | content | random_padding | content_length(2B) | checksum(4B) |

const (
	// BootSectorSize is the database boot sector's size.
	// The total length of boot is 4 KiB, enough for holding dozens databases,
	// limit the size of boot for atomic updating.
	// (actually device's page cloud be smaller than it, anyway it improves the rate of atomic operation, good news.)
	BootSectorSize = 4 * 1024

	BootSectorName = "db-boot"
)

type Boot struct {
	F  vfs.File
	DB *zmatrixpb.DBBoot

	buf []byte
}

func MakeBootPath(dbDir string) string {
	return filepath.Join(dbDir, BootSectorName)
}

// CreateDBBoot writes down a data block in a file as the bootstrap of a database.
func CreateDBBoot(fs vfs.FS, dbBootPath string, dbID uint32, engine zmatrixpb.DBEngine) (*Boot, error) {

	f, err := fs.Create(dbBootPath)
	if err != nil {
		return nil, err
	}

	b := &Boot{
		F: f,
		DB: &zmatrixpb.DBBoot{
			Id:     dbID,
			State:  zmatrixpb.DBState_DB_ReadWrite,
			Engine: engine,
		},
		buf: directio.AlignedBlock(BootSectorSize),
	}

	err = b.Flush()
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Flush boot sector to disk.
// Warn:
// Ensure it has been protected by lock before using.
func (b *Boot) Flush() error {

	out, err := proto.MarshalOptions{}.MarshalAppend(b.buf[:0], b.DB)
	if err != nil {
		return err
	}
	n := len(out)
	if n > BootSectorSize-6 {
		xlog.Errorf("database boot sector is out of 4KB: %d", n)
		return orpc.ErrInternalServer
	}

	binary.LittleEndian.PutUint16(b.buf[BootSectorSize-6:], uint16(n))

	cs := xdigest.Sum32(b.buf[:BootSectorSize-4])
	binary.LittleEndian.PutUint32(b.buf[BootSectorSize-4:], cs)

	_, err = b.F.WriteAt(b.buf, 0)
	if err != nil {
		return err
	}

	return b.F.Fdatasync()
}

func (b *Boot) SetState(s zmatrixpb.DBState) {

	atomic.StoreInt32((*int32)(&b.DB.State), int32(s))
}

func (b *Boot) GetState() zmatrixpb.DBState {
	return zmatrixpb.DBState(atomic.LoadInt32((*int32)(&b.DB.State)))
}

// LoadBoot loads database boot-sector file, returns *Boot
// Before return, it'll check the checksum.
func LoadBoot(fs vfs.FS, rootPath string) (b *Boot, err error) {

	fp := filepath.Join(rootPath, BootSectorName)
	f, err := fs.Open(fp)
	if err != nil {
		return nil, err
	}

	buf := directio.AlignedBlock(BootSectorSize)

	n, err := f.ReadAt(buf, 0)
	if err != nil {
		return nil, err
	}
	if n != BootSectorSize {
		xlog.Errorf("load database sector failed: mismatched size, exp: %d, got: %d", BootSectorSize, n)
		return nil, orpc.ErrInternalServer
	}

	csExp := binary.LittleEndian.Uint32(buf[BootSectorSize-4:])
	csAct := xdigest.Sum32(buf[:BootSectorSize-4])

	if csExp != csAct {
		return nil, xerrors.WithMessage(orpc.ErrChecksumMismatch, "db-boot-sector checksum mismatch")
	}

	mn := binary.LittleEndian.Uint16(buf[BootSectorSize-6:])

	b = &Boot{
		F:   f,
		DB:  new(zmatrixpb.DBBoot),
		buf: buf,
	}
	err = proto.Unmarshal(buf[:mn], b.DB)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Boot) Close() {

	_ = b.Flush()

	_ = b.F.Close()
}
