module g.tesamc.com/IT/zmatrix

go 1.15

require (
	g.tesamc.com/IT/keeper v0.0.0
	g.tesamc.com/IT/zaipkg v0.0.0
	g.tesamc.com/IT/zproto v0.0.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/cockroachdb/pebble v0.0.0-20210406181039-e3809b89b488
	github.com/elastic/go-hdrhistogram v0.1.0
	github.com/openacid/low v0.1.14
	github.com/openacid/slim v0.5.11
	github.com/spf13/cast v1.3.1
	github.com/stretchr/testify v1.6.1
	github.com/templexxx/bsegtree v0.0.3
	github.com/templexxx/tsc v1.1.1
	github.com/templexxx/xorsimd v0.4.1
)

// TODO GitLAB proxy issues
replace (
	g.tesamc.com/IT/keeper v0.0.0 => ../keeper
	g.tesamc.com/IT/zaipkg v0.0.0 => ../zaipkg
	g.tesamc.com/IT/zproto v0.0.0 => ../zproto
)
