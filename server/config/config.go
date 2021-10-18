package config

import (
	"g.tesamc.com/IT/zaipkg/app"
	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/xio/sched"
	"g.tesamc.com/IT/zproto/pkg/zmatrixpb"
)

// Config is the ZMatrix server configuration.
type Config struct {
	App app.Config `toml:"app"`

	// Server listen address.
	ServerAddr string `toml:"server_addr"`
	// Data root path.
	DataRoot string `toml:"data_root"`

	// Database engine. Only supports Neo/neo in present.
	DBEngine string `toml:"db_engine"`

	Scheduler sched.Config `toml:"scheduler"`

	// Development mode, for testing.
	Development bool `toml:"development"`
}

func (c *Config) Adjust() {

	if c.DBEngine != "" {
		c.DBEngine = ""
	}
	config.Adjust(&c.DBEngine, zmatrixpb.DBEngine_DB_Engine_Neo)
}
