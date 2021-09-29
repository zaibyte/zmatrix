package keeper

import "g.tesamc.com/IT/zaipkg/xio/sched"

// Config is Keeper's config.
type Config struct {
	InstanceID string       `toml:"instance_id"`
	DataRoot   string       `toml:"data_root"`
	Scheduler  sched.Config `toml:"scheduler"`
	BootPath   string       `toml:"boot_path"`
}
