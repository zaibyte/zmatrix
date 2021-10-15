package mgr

import (
	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/typeutil"
	"g.tesamc.com/IT/zaipkg/xio/sched"
)

// Config is Mgr's config.
type Config struct {
	InstanceID string       `toml:"instance_id"`
	DataRoot   string       `toml:"data_root"`
	Scheduler  sched.Config `toml:"scheduler"`
	// MinDiskSpace for creating a new database.
	MinDiskSpace typeutil.ByteSize `toml:"min_disk_space"`
}

const DefaultMinDiskSpace = typeutil.ByteSize(10 * 1024 * 1024 * 1024) // 10 GiB.

const (
	MaxDBNum = 16
)

func (c *Config) Adjust() {

	config.Adjust(&c.MinDiskSpace, DefaultMinDiskSpace)
}
