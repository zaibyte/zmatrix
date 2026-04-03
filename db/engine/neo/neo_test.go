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

	"github.com/zaibyte/zaipkg/vfs"
	"github.com/zaibyte/zaipkg/xio"
	_ "github.com/zaibyte/zaipkg/xlog/xlogtest"
	"github.com/zaibyte/zaipkg/xmath/xrand"
	"github.com/zaibyte/zmatrix/pkg/config"

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
	defer d.Close()

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
		if vLen == 0 {
			vLen = 1
		}
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
		if vLen == 0 {
			vLen = 1
		}
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

// 1. in transferring
// 2. after transferring
func TestDatabase_GetWithTrans(t *testing.T) {
	fs := testFS

	dbPath := filepath.Join(os.TempDir(), "neo.lv1", fmt.Sprintf("%d", xrand.Uint32()))

	err := fs.MkdirAll(dbPath, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll(dbPath)

	cnt := 128

	cfg := DefaultConfig
	cfg.ToLv1MaxEntries = uint64(cnt)

	d, err := Create(cfg, 0, dbPath, fs, &xio.NopScheduler{})
	if err != nil {
		t.Fatal(err)
	}
	err = d.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	kLen := 8 // fixed key length helping to accelerate testing, and the lengths of values are enough random.
	keyBuf := make([]byte, kLen)
	valBuf := make([]byte, config.MaxValueLen)

	rand.Seed(time.Now().UnixNano())
	rand.Read(valBuf) // We don't need too many value.

	kvs := make(map[uint64]uint32)

	for i := 0; i < cnt; i++ {

		binary.BigEndian.PutUint64(keyBuf, uint64(1024-i))

		// vLen := xrand.Uint32n(uint32(config.MaxValueLen))
		vLen := xrand.Uint32n(uint32(config.MaxValueLen / 4)) // Avoiding too slow testing.
		if vLen == 0 {
			vLen = 1
		}
		err = d.Set(keyBuf[:kLen], valBuf[:vLen])
		if err != nil {
			t.Fatal(err)
		}

		kvs[uint64(1024-i)] = vLen
	}

	kvs[uint64(1024-cnt)] = 1111 // fixed last element.

	wg2 := new(sync.WaitGroup)
	wg2.Add(2)

	go func() { // Trigger trans.
		defer wg2.Done()
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(1024-cnt))

		err = d.Set(key, valBuf[:1111])
		if err != nil {
			t.Error(err)
		}
	}()

	go func() {
		defer wg2.Done()
		key := make([]byte, 8)

		for {
			if !d.transUndone() {
				continue
			}

			// Trans has begun.

			for k, v := range kvs {
				binary.BigEndian.PutUint64(key, k)
				vact, closer, err := d.Get(key)
				if err != nil {
					t.Error(err)
				} else {
					assert.Equal(t, valBuf[:v], vact)

					_ = closer.Close()
				}
			}
			break
		}

		for {
			if d.transUndone() {
				continue
			}

			// Trans has done.
			for k, v := range kvs {
				binary.BigEndian.PutUint64(key, k)
				vact, closer, err := d.Get(key)
				if err != nil {
					t.Error(err)
				}

				assert.Equal(t, valBuf[:v], vact)

				_ = closer.Close()
			}
			break
		}

	}()

	wg2.Wait()

}

func TestDatabase_Load(t *testing.T) {
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
		if vLen == 0 {
			vLen = 1
		}
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

	err = d.Close()
	if err != nil {
		t.Fatal(err)
	}

	ld, err := Load(nil, 0, dbPath, fs, &xio.NopScheduler{}, false)
	if err != nil {
		t.Fatal(err)
	}
	err = ld.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer ld.Close()

	for k, v := range kvs {

		binary.BigEndian.PutUint64(keyBuf, k)
		vact, closer, err := ld.Get(keyBuf)
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
				vact, closer, err := ld.Get(key)
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

func TestDatabase_LoadAfterTrans(t *testing.T) {
	fs := testFS

	dbPath := filepath.Join(os.TempDir(), "neo.lv1", fmt.Sprintf("%d", xrand.Uint32()))

	err := fs.MkdirAll(dbPath, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll(dbPath)

	cnt := 128

	cfg := DefaultConfig
	cfg.ToLv1MaxEntries = uint64(cnt)

	d, err := Create(cfg, 0, dbPath, fs, &xio.NopScheduler{})
	if err != nil {
		t.Fatal(err)
	}
	err = d.Start()
	if err != nil {
		t.Fatal(err)
	}

	kLen := 8 // fixed key length helping to accelerate testing, and the lengths of values are enough random.
	keyBuf := make([]byte, kLen)
	valBuf := make([]byte, config.MaxValueLen)

	rand.Seed(time.Now().UnixNano())
	rand.Read(valBuf) // We don't need too many value.

	kvs := make(map[uint64]uint32)

	for i := 0; i < cnt; i++ {

		binary.BigEndian.PutUint64(keyBuf, uint64(1024-i))

		// vLen := xrand.Uint32n(uint32(config.MaxValueLen))
		vLen := xrand.Uint32n(uint32(config.MaxValueLen / 4)) // Avoiding too slow testing.
		if vLen == 0 {
			vLen = 1
		}
		err = d.Set(keyBuf[:kLen], valBuf[:vLen])
		if err != nil {
			t.Fatal(err)
		}

		kvs[uint64(1024-i)] = vLen
	}

	kvs[uint64(1024-cnt)] = 1111 // fixed last element.

	binary.BigEndian.PutUint64(keyBuf, uint64(1024-cnt))

	err = d.Set(keyBuf, valBuf[:1111])
	if err != nil {
		t.Error(err)
	}

	for {
		if d.transUndone() {
			time.Sleep(time.Microsecond * 100) // Wait until trans done.
			continue
		}
		break
	}

	err = d.Close()
	if err != nil {
		t.Fatal(err)
	}

	ld, err := Load(nil, 0, dbPath, fs, &xio.NopScheduler{}, false)
	if err != nil {
		t.Fatal(err)
	}
	err = ld.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer ld.Close()

	for k, v := range kvs {

		binary.BigEndian.PutUint64(keyBuf, k)
		vact, closer, err := ld.Get(keyBuf)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, valBuf[:v], vact)

		_ = closer.Close()
	}

}
