package neo

import (
	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/typeutil"
)

// Trigger for transferring entries to lv1 from lv0.
// Either of them could trigger transferring.
//
// If Database is in transferring and new transfer should be trigged by these conditions,
// Database will return zmerrors.ErrTooFastSet.
const (
	// DefaultToLv1Threshold is 8 GiB.
	DefaultToLv1Threshold  = typeutil.ByteSize(8 * 1024 * 1024 * 1024)
	DefaultToLv1MaxEntries = uint64(4194304) // Will cost ~2s for sorting all keys (8bytes each)
)

type Config struct {
	ToLv1Threshold  typeutil.ByteSize `toml:"to_lv1_threshold"`
	ToLv1MaxEntries uint64            `toml:"to_lv_1_max_entries"`
}

var DefaultConfig = &Config{
	ToLv1MaxEntries: DefaultToLv1MaxEntries,
	ToLv1Threshold:  DefaultToLv1Threshold,
}

func (c *Config) Adjust() {

	config.Adjust(&c.ToLv1Threshold, DefaultToLv1Threshold)
	config.Adjust(&c.ToLv1MaxEntries, DefaultToLv1MaxEntries)
}
