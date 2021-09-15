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
	"encoding/binary"
	"io"
	"time"

	"g.tesamc.com/IT/zaipkg/xbytes"
)

const (
	// DefaultPendingMessages is the default number of pending messages
	// handled by Client and Server.
	//
	// It's meaningless for setting it too big for unix domain socket.
	DefaultPendingMessages = uint64(4096)

	// DefaultFlushDelay Sacrifice the number of Write() calls to the smallest
	// possible latency, since it has higher priority in local IPC.
	DefaultFlushDelay = time.Duration(-1)
	DefaultNoReqSleep = 10 * time.Microsecond
)

// compactSetBatchReq compacts keys & values into one byte slice for sending to server.
// len(keys) must equal to len(values).
//
// Caller has the responsibility to put return value back into bytes pool by invoke Close().
func compactSetBatchReq(keys, values [][]byte) ([]byte, io.Closer) {

	cnt := len(keys)
	keysLen, valsLen := 0, 0
	for _, key := range keys {
		keysLen += len(key)
	}
	for _, val := range values {
		valsLen += len(val)
	}

	vLen := 4 + 2*cnt + 4*cnt + keysLen + valsLen
	v := xbytes.GetBytes(vLen)

	binary.BigEndian.PutUint32(v[:4], uint32(cnt))

	offset := 4 + 2*cnt + 4*cnt
	for i, key := range keys {
		kl := len(key)
		binary.BigEndian.PutUint16(v[4+i*2:4+i*2+2], uint16(kl))
		copy(v[offset:], key)
		offset += kl
	}
	for i, v0 := range values {
		vl := len(v0)
		binary.BigEndian.PutUint32(v[4+2*cnt+i*4:4+2*cnt+i*4+4], uint32(vl))
		copy(v[offset:], v0)
		offset += vl
	}

	return v, PoolBytesCloser{v}
}

func extraSetBatchReq(c []byte) (keys, values [][]byte) {

	cnt := int(binary.BigEndian.Uint32(c[:4]))

	keys = make([][]byte, cnt)
	values = make([][]byte, cnt)

	offset := 4 + 2*cnt + 4*cnt
	for i := range keys {
		kl := int(binary.BigEndian.Uint16(c[4+i*2 : 4+i*2+2]))
		keys[i] = c[offset : offset+kl]
		offset += kl
	}
	for i := range values {
		vl := int(binary.BigEndian.Uint32(c[4+cnt*2+i*4 : 4+cnt*4+i*4+4]))
		values[i] = c[offset : offset+vl]
		offset += vl
	}
	return
}

type PoolBytesCloser struct {
	p []byte
}

func (r PoolBytesCloser) Close() error {
	xbytes.PutBytes(r.p)
	return nil
}
