// Copyright (c) 2020. Temple3x (temple3x@gmail.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// The MIT License (MIT)
//
// Copyright (c) 2014 Aliaksandr Valialkin
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// This file contains code derived from gorpc.
// The main logic & codes are copied from gorpc.

package urpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xbytes"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zaipkg/xmath/xrand"

	"github.com/templexxx/tsc"
)

var randVal = directio.AlignedBlock(1024)

func init() {
	rand.Seed(tsc.UnixNano())
	rand.Read(randVal)
}

type testHandler struct {
	setFn func(db uint32, key, value []byte) error
	getFn func(db uint32, key []byte) (value []byte, err error)
}

func (h *testHandler) Set(db uint32, key, value []byte) error {
	return h.setFn(db, key, value)
}

func (h *testHandler) Get(db uint32, key []byte) (value []byte, err error) {
	return h.getFn(db, key)
}

func nopHandler() *testHandler {
	return &testHandler{
		setFn: func(db uint32, key, value []byte) error {
			return nil
		},
		getFn: func(db uint32, key []byte) (value []byte, err error) {
			return nil, nil
		},
	}
}

func getRandomAddr() string {
	return fmt.Sprintf("./test-%d.sock", xrand.Uint32n(20000)+10000)
}

func getRandomTCPAddr() string {
	rand.Seed(tsc.UnixNano())
	return fmt.Sprintf("127.0.0.1:%d", rand.Intn(20000)+10000)
}

func newTestClient(addr string) *Client {
	c := NewClient(addr, 0)
	c.CloseWait = time.Microsecond
	return c
}

func TestClient_Get(t *testing.T) {
	addr := getRandomAddr()

	exp := make(map[uint64][]byte) // For testing, key is an uint64.

	h := nopHandler()
	h.setFn = func(db uint32, key, value []byte) error {
		o := make([]byte, len(value))
		copy(o, value)
		k := binary.LittleEndian.Uint64(key)
		exp[k] = o
		return nil
	}
	h.getFn = func(db uint32, key []byte) (value []byte, err error) {
		k := binary.LittleEndian.Uint64(key)
		o := exp[k]
		size := len(o)
		value = xbytes.GetBytes(int(size))
		copy(value, o)
		return
	}

	s := NewServer(addr, h)
	if err := s.Start(); err != nil {
		t.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop(nil)

	c := newTestClient(addr)
	err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop(nil)

	key := make([]byte, 8)
	for i := 0; i < 128; i++ {

		size := xrand.Uint32n(uint32(len(randVal) + 1))
		if size == 0 {
			size = 1
		}
		val := randVal[:size]
		binary.LittleEndian.PutUint64(key, uint64(i))
		err := c.Set(key, val)
		if err != nil {
			t.Fatal(err, size)
		}
	}

	for k, v := range exp {
		binary.LittleEndian.PutUint64(key, k)
		act, closer, err := c.Get(key)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(act, v) {
			t.Fatal("obj data mismatch")
		}
		closer.Close()
	}
}

func TestClient_Get_Concurrency(t *testing.T) {
	addr := getRandomAddr()

	exp := new(sync.Map)

	h := nopHandler()
	h.setFn = func(db uint32, key, value []byte) error {

		o := make([]byte, len(value))
		copy(o, value)
		exp.Store(binary.LittleEndian.Uint64(key), o)
		return nil
	}
	h.getFn = func(db uint32, key []byte) (value []byte, err error) {

		o, ok := exp.Load(binary.LittleEndian.Uint64(key))
		if !ok {
			return nil, orpc.ErrNotFound
		}
		v := o.([]byte)

		value = xbytes.GetBytes(len(v))

		copy(value, o.([]byte))
		return
	}

	s := NewServer(addr, h)
	if err := s.Start(); err != nil {
		t.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop(nil)

	c := newTestClient(addr)
	err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop(nil)

	testCnt := 128
	keys := make([][]byte, testCnt)
	for i := 0; i < testCnt; i++ {

		size := xrand.Uint32n(uint32(len(randVal) + 1))
		if size == 0 {
			size = 1
		}
		val := randVal[:size]
		ku := uint64(i)
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, ku)
		err := c.Set(key, val)
		if err != nil {
			t.Fatal(err, size)
		}
		keys[i] = key
	}

	errC := make(chan error, testCnt)

	var wg sync.WaitGroup
	for _, k := range keys {
		wg.Add(1)
		go func(key []byte) {
			defer wg.Done()

			act, closer, err := c.Get(key)
			if err != nil {
				errC <- err
				return
			}

			v2, ok := exp.Load(binary.LittleEndian.Uint64(key))
			if !ok {
				errC <- errors.New("not found")
				closer.Close()
				return
			}
			if !bytes.Equal(act, v2.([]byte)) {
				errC <- errors.New("get obj data mismatch")
				closer.Close()
				return
			}

			act, closer, err = c.Get([]byte("not found"))
			if !errors.Is(err, orpc.ErrNotFound) {
				errC <- errors.New("should be not found")
				return
			}

		}(k)
	}

	wg.Wait()

	close(errC)
	for err := range errC {
		t.Error(err)
	}
}
