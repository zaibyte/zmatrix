package mgr

import (
	"path/filepath"
	"strings"

	"g.tesamc.com/IT/zaipkg/vdisk/svr"
)

// GetDiskIDFromPath gets diskID from:
// <data_root>/disk_<disk_id>
func GetDiskIDFromPath(diskPath string) string {

	fn := filepath.Base(diskPath)
	return strings.TrimPrefix(fn, svr.DiskNamePrefix)
}
