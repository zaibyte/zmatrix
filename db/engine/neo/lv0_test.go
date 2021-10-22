package neo

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestBatch(t *testing.T) {

	dir, err := ioutil.TempDir(os.TempDir(), "pebble")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b := db.NewBatch()
	err = b.Set([]byte("hello"), []byte("world"), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = b.Set([]byte("hello1"), []byte("world1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = b.Commit(pebble.Sync)
	if err != nil {
		t.Fatal(err)
	}
	err = b.Close()
	if err != nil {
		t.Fatal(err)
	}

	s := db.NewSnapshot()

	iter := s.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()
		fmt.Printf("iter %s %s\n", k, v)
	}
	err = iter.Close()
	if err != nil {
		t.Fatal(err)
	}
}
