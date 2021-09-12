package config

import (
	"g.tesamc.com/IT/zaipkg/app"
	"g.tesamc.com/IT/zaipkg/xio/sched"
)

// Config is the ZMatrix server configuration.
type Config struct {
	App app.Config `toml:"app"`

	// Server listen address.
	ServerAddr string `toml:"server_addr"`
	// Data root path.
	DataRoot string `toml:"data_root"`

	Scheduler sched.Config `toml:"scheduler"`

	// Development mode, for testing.
	Development bool `toml:"development"`
}

func (c *Config) Adjust() {

}
