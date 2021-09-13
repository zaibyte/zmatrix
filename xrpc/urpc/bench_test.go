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
	"math/rand"
	"testing"
	"time"

	"g.tesamc.com/IT/zaipkg/xbytes"
)

// TODO if you want to run bench, should raise the xbytes leaky pool capacities.
// which has been adjusted to smaller numbers.

func BenchmarkClient_Set(b *testing.B) {

	rand.Seed(time.Now().UnixNano())

	addr := getRandomAddr()

	s := NewServer(addr, nopHandler())

	if err := s.Start(); err != nil {
		b.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop(nil)

	c := newTestClient(addr)
	c.Conns = 4

	err := c.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Stop(nil)

	value := make([]byte, 1024)
	rand.Read(value)
	key := make([]byte, 8)
	rand.Read(key)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			err := c.Set(key, value)
			if err != nil {
				b.Fatalf("Unexpected error: %s", err)
			}
		}
	})
}

func newBenchGetHandler() *testHandler {

	return &testHandler{
		setFn: func(db uint32, key, value []byte) error {
			return nil
		},
		getFn: func(db uint32, key []byte) (value []byte, err error) {
			value = xbytes.GetBytes(1024)
			return
		},
	}
}

func BenchmarkClient_Get(b *testing.B) {

	rand.Seed(time.Now().UnixNano())

	addr := getRandomAddr()

	s := NewServer(addr, newBenchGetHandler())

	if err := s.Start(); err != nil {
		b.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop(nil)

	c := newTestClient(addr)
	c.Conns = 4

	err := c.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Stop(nil)

	key := make([]byte, 8)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			val, closer, err := c.Get(key)
			if err != nil {
				b.Fatalf("Unexpected error: %s", err)
			}
			xbytes.PutBytes(val)
			closer.Close()
		}
	})
}
