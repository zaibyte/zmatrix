package neo

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zaipkg/xmath/xrand"
	"g.tesamc.com/IT/zmatrix/pkg/config"

	"github.com/stretchr/testify/assert"
)

var (
	testFS = vfs.GetTestFS()
)

func TestDatabase_Get(t *testing.T) {

	fs := testFS

	dbPath := filepath.Join(os.TempDir(), "neo.lv1", fmt.Sprintf("%d", xrand.Uint32()))

	err := fs.MkdirAll(dbPath, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll(dbPath)

	d, err := Create(nil, 0, dbPath, fs, &xio.NopScheduler{})
	if err != nil {
		t.Fatal(err)
	}
	err = d.Start()
	if err != nil {
		t.Fatal(err)
	}

	cnt := 128
	kLen := 8 // fixed key length helping to accelerate testing, and the lengths of values are enough random.
	keyBuf := make([]byte, kLen)
	valBuf := make([]byte, config.MaxValueLen)

	rand.Seed(time.Now().UnixNano())
	rand.Read(valBuf) // We don't need too many value.

	kvs := make(map[uint64]uint32)

	for i := 0; i < cnt/2; i++ {

		binary.BigEndian.PutUint64(keyBuf, uint64(1024-i))

		// vLen := xrand.Uint32n(uint32(config.MaxValueLen))
		vLen := xrand.Uint32n(uint32(config.MaxValueLen / 4)) // Avoiding too slow testing.

		err = d.Set(keyBuf[:kLen], valBuf[:vLen])
		if err != nil {
			t.Fatal(err)
		}

		kvs[uint64(1024-i)] = vLen
	}

	keys := make([][]byte, cnt/2)
	values := make([][]byte, cnt/2)
	for i := cnt / 2; i < cnt; i++ {
		keys[i-cnt/2] = make([]byte, kLen)
		binary.BigEndian.PutUint64(keys[i-cnt/2], uint64(1024-i))
		vLen := xrand.Uint32n(uint32(config.MaxValueLen / 4)) // Avoiding too slow testing.
		values[i-cnt/2] = make([]byte, vLen)
		copy(values[i-cnt/2], valBuf)

		kvs[uint64(1024-i)] = vLen
	}

	err = d.SetBatch(keys, values)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range kvs {

		binary.BigEndian.PutUint64(keyBuf, k)
		vact, closer, err := d.Get(keyBuf)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, valBuf[:v], vact)

		_ = closer.Close()
	}

	wg := new(sync.WaitGroup)
	wg.Add(4)

	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()

			key := make([]byte, 8)
			for k, v := range kvs {

				binary.BigEndian.PutUint64(key, k)
				vact, closer, err := d.Get(key)
				if err != nil {
					t.Error(err)
				}

				assert.Equal(t, valBuf[:v], vact)

				_ = closer.Close()
			}
		}()
	}

	wg.Wait()

}
