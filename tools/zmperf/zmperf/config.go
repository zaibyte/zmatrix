package zmperf

import (
	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/typeutil"
)

type Config struct {
	DataRoot string `toml:"data_root"`

	// If ValSize > 512KB, set it to default.
	ValSize typeutil.ByteSize `toml:"val_size"`
	// TODO add various engine supports
	JobType string `toml:"job_type"`
	jobType int
	JobTime int64 `toml:"job_time"` // sec
	// Ignore first SkipTime when collect result.
	SkipTime int64 `toml:"skip_time"`

	// MBPer_XXX_Thread * threads = total_IO.
	MBPerGetThread int `toml:"mb_per_get_thread"`
	GetThreads     int `toml:"get_threads"`

	// Scheduler configs.
	IOThreads int `toml:"io_threads"`

	NopSched bool `toml:"nop_sched"`
	// IsDoNothing will do nothing at all when there should be a I/O request.
	// It's used for measuring the zMatrix works as expect.
	IsDoNothing bool `toml:"is_do_nothing"`

	// When JobType is rpc, pls start zMatrix in another process with ServerAddr.
	ServerAddr string `toml:"server_addr"`

	// PrintLog prints log to stdout or not.
	PrintLog bool `toml:"print_log"`

	PrepareDone bool `toml:"prepare_done"`

	// Ignore any error, by default process will fatal when there is a not found error.
	IgnoreError bool `toml:"ignore_error"`
}

var jobTypes = map[string]int{
	"embed": Embed,
	"rpc":   RPC,
}

const (
	RPC   = 1
	Embed = 2
)

const DefaultValSize = typeutil.ByteSize(900) // Not aligned, so what? :D

func (c *Config) adjust() {

	if c.ValSize > 512*1024 {
		c.ValSize = 0
	}

	config.Adjust(&c.ValSize, DefaultValSize)
	config.Adjust(&c.JobType, "embed")
	config.Adjust(&c.JobTime, 300)
	config.Adjust(&c.SkipTime, 10)
}
