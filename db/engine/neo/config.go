package neo

import (
	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/typeutil"
)

// Trigger for transferring entries to lv1 from lv0.
// Either of them could trigger transferring.
const (
	// DefaultToLv1Threshold is 8 GiB.
	DefaultToLv1Threshold  = 8 * 1024 * 1024 * 1024
	DefaultToLv1MaxEntries = 4194304 // Will cost ~2s for sorting all keys (8bytes each)
)

type Config struct {
	ToLv1Threshold typeutil.ByteSize `toml:"to_lv1_threshold"`
}

func (c *Config) Adjust() {

	config.Adjust(&c.ToLv1Threshold, DefaultToLv1Threshold)
}
