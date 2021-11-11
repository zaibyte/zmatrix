package mgr

import (
	"time"

	"g.tesamc.com/IT/zmatrix/db/engine/neo"

	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/typeutil"
	"g.tesamc.com/IT/zaipkg/xio/sched"
)

// Config is Mgr's config.
type Config struct {
	InstanceID string        `toml:"instance_id"`
	DataRoot   string        `toml:"data_root"`
	Scheduler  *sched.Config `toml:"scheduler"`
	// MinDiskSpace for creating a new database.
	MinDiskSpace typeutil.ByteSize `toml:"min_disk_space"`
	// Database engine when creating new one. Only supports Neo/neo in present.
	DBEngine string `toml:"db_engine"`
	// UpdateStateDuration is the duration between updating all databases' state.
	UpdateStateDuration typeutil.Duration `toml:"update_state_duration"`
	NeoConfig           neo.Config        `toml:"neo_config"`
}

const (
	DefaultMinDiskSpace = typeutil.ByteSize(10 * 1024 * 1024 * 1024) // 10 GiB.
)

var (
	DefaultUpdateStateDuration = 5 * time.Minute
)

func (c *Config) Adjust() {

	config.Adjust(&c.MinDiskSpace, DefaultMinDiskSpace)
	config.Adjust(&c.DBEngine, "neo")
	config.Adjust(&c.UpdateStateDuration, DefaultUpdateStateDuration)
	c.NeoConfig.Adjust()
}
