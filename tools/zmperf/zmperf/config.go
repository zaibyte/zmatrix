package zmperf

import "g.tesamc.com/IT/zaipkg/typeutil"

type Config struct {
	DataRoot string `toml:"data_root"`

	ValSize typeutil.ByteSize `toml:"val_size"`
	// TODO add various engine supports
	JobType string `toml:"job_type"`
	Embed   bool   `toml:"embed"`
	RPC     bool   `toml:"rpc"`
	JobTime int64  `toml:"job_time"` // sec
	// Ignore first SkipTime when collect result.
	SkipTime int64 `toml:"skip_time"`

	// MBPer_XXX_Thread * threads = total_IO.
	MBPerPutThread int `toml:"mb_per_put_thread"`
	MBPerGetThread int `toml:"mb_per_get_thread"`
	PutThreads     int `toml:"put_threads"`
	GetThreads     int `toml:"get_threads"`

	// Scheduler configs.
	IOThreads int `toml:"io_threads"`

	// IsDoNothing will do nothing at all when there should be a I/O request.
	// It's used for measuring the zMatrix works as expect.
	IsDoNothing bool `toml:"is_do_nothing"`

	// PrintLog prints log to stdout or not.
	PrintLog bool `toml:"print_log"`
}

var jobTypes = map[string]int{
	"embed": Embed,
	"rpc":   RPC,
}

const (
	RPC   = 1
	Embed = 2
)

const DefaultValSize = typeutil.ByteSize(900)
