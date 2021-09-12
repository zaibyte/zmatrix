module g.tesamc.com/IT/zmatrix

go 1.17

require (
	g.tesamc.com/IT/zaipkg v0.0.0
	github.com/cockroachdb/pebble v0.0.0-20210406181039-e3809b89b488
	github.com/panjf2000/ants/v2 v2.4.4-0.20210318172516-2e763f12162d
	github.com/stretchr/testify v1.6.1
	github.com/templexxx/tsc v0.0.3
)

require (
	g.tesamc.com/IT/zproto v0.0.0 // indirect
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cockroachdb/errors v1.8.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3-0.20201103224600-674baa8c7fc3 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/gyuho/linux-inspect v0.0.0-20180929231013-a492bfc5f12a // indirect
	github.com/jaypipes/ghw v0.8.0 // indirect
	github.com/julienschmidt/httprouter v1.2.0 // indirect
	github.com/klauspost/compress v1.11.7 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lni/goutils v1.3.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/templexxx/cpu v0.0.8-0.20210423085042-1c810926b5dd // indirect
	github.com/templexxx/fnc v1.0.1 // indirect
	github.com/urfave/negroni/v2 v2.0.2 // indirect
	github.com/zaibyte/nanozap v0.0.6 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/exp v0.0.0-20201210212021-a20c86df00b4 // indirect
	golang.org/x/sys v0.0.0-20210514084401-e8d321eab015 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/yaml.v2 v2.3.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
	howett.net/plist v0.0.0-20181124034731-591f970eefbb // indirect
)

// TODO GitLAB proxy issues
replace (
	g.tesamc.com/IT/keeper v0.0.0 => ../keeper
	g.tesamc.com/IT/zai v0.0.0 => ../zai
	g.tesamc.com/IT/zaipkg v0.0.0 => ../zaipkg
	g.tesamc.com/IT/zproto v0.0.0 => ../zproto
)
