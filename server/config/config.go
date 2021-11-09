package config

import (
	"g.tesamc.com/IT/zaipkg/app"
	"g.tesamc.com/IT/zmatrix/mgr"
)

// Config is the ZMatrix server configuration.
type Config struct {
	App app.Config `toml:"app"`

	// Server listen address.
	ServerAddr string `toml:"server_addr"`

	Manager mgr.Config `toml:"manager"`

	// Embed is true, RPC will be disabled.
	Embed bool `toml:"embed"`

	// Development mode, for testing.
	Development bool `toml:"development"`
}

func (c *Config) Adjust() {

	c.App.Adjust()
	c.Manager.Adjust()
}
