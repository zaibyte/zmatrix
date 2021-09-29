package keeper

import (
	"strconv"
	"testing"

	"g.tesamc.com/IT/zmatrix/db"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/vfs"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zaipkg/xmath/xrand"
	"g.tesamc.com/IT/zmatrix/pkg/zmerrors"

	"github.com/stretchr/testify/assert"
)

func TestCreateKpBoot(t *testing.T) {

	fs := vfs.GetTestFS()

	err := fs.MkdirAll("root", 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll("root")

	kpr, err := CreateKpBoot(fs, "root")
	if err != nil {
		t.Fatal(err)
	}
	kpr.Close()
}

func TestKeeperBoot_Add(t *testing.T) {
	fs := vfs.GetTestFS()

	err := fs.MkdirAll("root", 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll("root")

	kpr, err := CreateKpBoot(fs, "root")
	if err != nil {
		t.Fatal(err)
	}
	defer kpr.Close()

	m := make(map[uint32]string)
	for i := 0; i < 100; i++ {
		m[uint32(i)] = strconv.Itoa(i)
	}

	for id, p := range m {
		err = kpr.Add(id, p)
		if err != nil {
			t.Fatal(err)
		}
	}

	for id, p := range m {
		act, err := kpr.Search(id)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, p, act)
	}
}

func TestKeeperBoot_Del(t *testing.T) {
	fs := vfs.GetTestFS()

	err := fs.MkdirAll("root", 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll("root")

	kpr, err := CreateKpBoot(fs, "root")
	if err != nil {
		t.Fatal(err)
	}
	defer kpr.Close()

	m := make(map[uint32]string)
	for i := 0; i < 100; i++ {
		m[uint32(i)] = strconv.Itoa(i)
	}

	for id, p := range m {
		err = kpr.Add(id, p)
		if err != nil {
			t.Fatal(err)
		}
	}

	del := xrand.Uint32n(100)
	delete(m, del)

	err = kpr.Del(del)
	if err != nil {
		t.Fatal(err)
	}

	for id, p := range m {
		act, err := kpr.Search(id)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, p, act)
	}

	act, err := kpr.Search(del)
	assert.Equal(t, "", act)
	assert.Equal(t, orpc.ErrNotFound, err)
}

func TestFlushKpBoot_NoSpace(t *testing.T) {

	fs := vfs.GetTestFS()

	err := fs.MkdirAll("root", 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll("root")

	kpr, err := CreateKpBoot(fs, "root")
	if err != nil {
		t.Fatal(err)
	}
	defer kpr.Close()

	cnt := 527

	m := make(map[uint32]string)
	for i := 0; i < cnt; i++ {
		m[uint32(i)] = "a"
	}

	n := 0
	for i := 0; i < cnt; i++ {
		err = kpr.Add(uint32(i), m[uint32(i)])
		if err != nil {
			t.Fatal(err, n)
		}
		n++
	}

	for id, p := range m {
		act, err := kpr.Search(id)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, p, act)
	}

	err = kpr.Add(uint32(cnt), "cannot be added")
	assert.Equal(t, zmerrors.ErrTooManyDatabase, err)
}

func TestLoadKpBoot(t *testing.T) {

	fs := vfs.GetTestFS()

	err := fs.MkdirAll("root", 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll("root")

	kpr, err := CreateKpBoot(fs, "root")
	if err != nil {
		t.Fatal(err)
	}
	defer kpr.Close()

	cnt := 527

	m := make(map[uint32]string)
	for i := 0; i < cnt; i++ {
		m[uint32(i)] = "a"
	}

	n := 0
	for i := 0; i < cnt; i++ {
		err = kpr.Add(uint32(i), m[uint32(i)])
		if err != nil {
			t.Fatal(err, n)
		}
		n++
	}

	for id, p := range m {
		act, err := kpr.Search(id)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, p, act)
	}

	err = kpr.Add(uint32(cnt), "cannot be added")
	assert.Equal(t, zmerrors.ErrTooManyDatabase, err)

	lkp, err := db.LoadKpBoot(fs, "root")
	if err != nil {
		t.Fatal(err)
	}
	defer lkp.Close()

	for id, p := range m {
		act, err := kpr.Search(id)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, p, act)
	}
}
