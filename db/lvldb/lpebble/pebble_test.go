package lpebble

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestSnapshot(t *testing.T) {

	dir, err := ioutil.TempDir(os.TempDir(), "pebble")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("hello")
	if err := db.Set(key, []byte("world"), pebble.Sync); err != nil {
		t.Fatal(err)
	}

	value, closer, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%s %s\n", key, value)

	s := db.NewSnapshot()

	key2 := []byte("hello2")
	if err := db.Set(key2, []byte("world2"), pebble.Sync); err != nil {
		t.Fatal(err)
	}

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

	s = db.NewSnapshot()

	iter = s.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()
		fmt.Printf("iter2 %s %s\n", k, v)
	}
	err = iter.Close()
	if err != nil {
		t.Fatal(err)
	}

	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

}

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
