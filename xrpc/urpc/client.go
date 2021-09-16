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
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"g.tesamc.com/IT/zaipkg/xtest"

	"g.tesamc.com/IT/zaipkg/xmath"

	"g.tesamc.com/IT/zaipkg/limitring"

	"g.tesamc.com/IT/zaipkg/xerrors"

	"g.tesamc.com/IT/zaipkg/xbytes"

	"g.tesamc.com/IT/zmatrix/xrpc"

	"g.tesamc.com/IT/zaipkg/config"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xlog"
)

// Client implements xrpc.Client.
//
// The client must be started with Client.Start() before use.
//
// It is absolutely safe and encouraged using a single client across arbitrary
// number of concurrently running goroutines.
//
// Default client settings are optimized for high load, so don't override
// them without valid reason.
type Client struct {
	isRunning int64

	DB uint32

	// Server address to connect to.
	Addr string

	// The number of concurrent connections the client should establish
	// to the sever.
	// Default is DefaultClientConns.
	Conns uint64

	// The maximum number of pending requests in the queue.
	//
	// The number of pending requests should exceed the expected number
	// of concurrent goroutines calling client's methods.
	// Otherwise a lot of orpc.ErrRequestQueueOverflow errors may appear.
	//
	// Default is DefaultPendingMessages.
	PendingRequests uint64

	// The client calls this callback when it needs new connection
	// to the server.
	// The client passes Client.Addr into Dial().
	//
	// By default it returns UNIX connections established to the Client.Addr.
	Dial DialFunc

	nextConn     uint64
	reqQueue     *limitring.Ring
	connPool     []net.Conn
	requestsChan []chan *AsyncResult

	wg sync.WaitGroup
}

var _client xrpc.Client = new(Client)

// AsyncResult is a result returned from Client.callAsync().
type AsyncResult struct {
	Method   uint8
	ReqKey   []byte
	ReqValue []byte

	RespValue []byte

	Err chan error
}

const (
	DefaultDB = uint32(1)

	// DefaultClientConns is the default connection numbers for Client.
	DefaultClientConns = uint64(16)
)

// Start starts rpc client. Establishes connection to the server on Client.Addr.
func (c *Client) Start() error {

	if atomic.LoadInt64(&c.isRunning) == 1 {
		xlog.Debug("urpc client already started")
		return nil
	}

	config.Adjust(&c.DB, DefaultDB)
	config.Adjust(&c.PendingRequests, DefaultPendingMessages)
	config.Adjust(&c.Conns, DefaultClientConns)

	c.PendingRequests = xmath.NextPower2(c.PendingRequests / 2)

	// Start write index at the value before 0
	// to allow the first conn to use AddUint64
	// and still have a beginning index of 0
	c.nextConn = ^c.nextConn
	c.reqQueue = limitring.New(c.PendingRequests)
	c.requestsChan = make([]chan *AsyncResult, c.Conns)
	for i := range c.requestsChan {
		c.requestsChan[i] = make(chan *AsyncResult, c.PendingRequests/2/c.Conns)
	}

	c.connPool = make([]net.Conn, c.Conns)

	if c.Dial == nil {
		c.Dial = defaultDial
	}

	for i := uint64(0); i < c.Conns; i++ { // There is no reason that failed to establish UDS conn except serious issue.
		conn, err := c.Dial(c.Addr)
		if err != nil {
			c.closeAllConn()
			return err
		}
		c.connPool[i] = conn
	}

	atomic.StoreInt64(&c.isRunning, 1)

	c.wg.Add(1)
	go c.dispatch()

	for i := uint64(0); i < c.Conns; i++ {
		c.wg.Add(1)
		go c.runReqWorker(int(i))
	}

	return nil
}

func (c *Client) closeAllConn() {

	for _, co := range c.connPool {
		if co != nil {
			_ = co.Close()
		}
	}
}

func (c *Client) Stop(_ error) {
	if !atomic.CompareAndSwapInt64(&c.isRunning, 1, 0) {
		return
	}

	c.wg.Wait()

	c.closeAllConn()
}

func (c *Client) Set(key, value []byte) error {
	_, _, err := c.call(setMethod, key, value)
	return err
}

func (c *Client) SetBatch(keys, values [][]byte) error {
	kCnt := len(keys)
	vCnt := len(values)
	if kCnt != vCnt {
		return xerrors.WithMessage(orpc.ErrBadRequest, "keys & values count must be equal for set batch")
	}

	value, closer := compactSetBatchReq(keys, values)
	defer closer.Close()
	_, _, err := c.call(setBatchMethod, nil, value)
	return err
}

func (c *Client) Get(key []byte) ([]byte, io.Closer, error) {

	return c.call(getMethod, key, nil)
}

// call sends the given request to the server and obtains response
// from the server.
//
// Returns non-nil error if the response cannot be obtained.
//
// Don't forget starting the client with Client.Start() before calling Client.call().
func (c *Client) call(method uint8, key, value []byte) ([]byte, io.Closer, error) {

	if atomic.LoadInt64(&c.isRunning) != 1 {
		return nil, nil, orpc.ErrServiceClosed
	}

	var ar *AsyncResult
	var err error
	if ar, err = c.callAsync(method, key, value); err != nil {
		return nil, nil, err
	}

	err = <-ar.Err
	if err != nil {
		ReleaseAsyncResult(ar)
		return nil, nil, err
	}
	if ar.RespValue != nil {
		v := ar.RespValue
		ReleaseAsyncResult(ar)
		return v, PoolBytesCloser{v}, nil
	}

	return nil, nil, nil
}

func (c *Client) callAsync(method uint8, key, value []byte) (ar *AsyncResult, err error) {

	if method != setMethod && method != getMethod {
		return nil, orpc.ErrNotImplemented
	}

	ar = AcquireAsyncResult()

	ar.Method = method
	ar.ReqKey = key
	ar.Err = make(chan error)

	if method == setMethod || method == setBatchMethod {
		ar.ReqValue = value
	}

	err = c.reqQueue.Push(unsafe.Pointer(ar))
	if err != nil {
		ReleaseAsyncResult(ar)
		return nil, err // Queue is full.
	}
	return ar, nil
}

func (c *Client) dispatch() {
	defer c.wg.Done()

	q := c.reqQueue

	maxSleep := time.Microsecond * 10
	retry := &orpc.Retryer{
		MinSleep: time.Nanosecond * 50, // About one ->chan-> operation.
		MaxTried: 10,
		MaxSleep: maxSleep,
	}

	lastSleepIter := 0
	for i := 0; ; i++ {
		if atomic.LoadInt64(&c.isRunning) != 1 {
			return
		}

		d, ok := q.Pop()
		if !ok {
			sleepDuration := retry.GetSleepDuration(i-lastSleepIter, 0)
			if sleepDuration >= maxSleep {
				time.Sleep(sleepDuration)
				lastSleepIter = i
				continue
			}
			spins := durationToSpins(sleepDuration)
			// TODO may spin too much?
			xtest.DoNothing(spins) // Using spin but not sleep to reduce context switch & runtime overhead hugely.
			continue
		}

		ar := (*AsyncResult)(d)
		c.nextConn++
		idx := c.nextConn % c.Conns
		select {
		case c.requestsChan[idx] <- ar:
		default:
			select {
			case c.requestsChan[idx] <- ar:
			default:
				ar.Err <- orpc.ErrRequestQueueOverflow
			}
		}
	}
}

func (c *Client) runReqWorker(i int) {

	conn := c.connPool[i]
	reqC := c.requestsChan[i]

	reqH := new(reqHeader)
	reqBuf := make([]byte, 64*1024)

	respH := new(respHeader)
	respHBuf := make([]byte, respHeaderSize)

	var err error
	for {
		if atomic.LoadInt64(&c.isRunning) != 1 {
			break
		}

		ar := <-reqC

		reqH.method = ar.Method
		reqH.keySize = uint16(len(ar.ReqKey))
		reqH.dbID = c.DB
		if ar.ReqValue != nil {
			reqH.valueSize = uint32(len(ar.ReqValue))
		} else {
			reqH.valueSize = 0
		}

		err = encodeToConn(conn, reqH, ar.ReqKey, ar.ReqValue, reqBuf, true)
		if err != nil { // I don't think re-connect to a UDS is a good idea. Just return error to user.
			ar.Err <- err
			break
		}

		// Waiting for response.
		_, err = readAtLeast(conn, respHBuf, respHeaderSize)
		if err != nil {
			ar.Err <- err
			break
		}

		_ = respH.decode(respHBuf)

		errno := respH.errno
		if errno != 0 { // Ignore response if any error. And the response must be nil.
			ar.Err <- orpc.Errno(errno).ToErr()
			continue
		}

		n := respH.bodySize
		if n == 0 {
			ar.Err <- nil
			continue
		}

		if n != 0 {
			ar.RespValue = xbytes.GetBytes(int(n))
			_, err = readAtLeast(conn, ar.RespValue, int(n))
			if err != nil { // If failed to read body, the next read header will be failed too, so just return.
				xbytes.PutBytes(ar.RespValue)
				ar.Err <- err
				break
			}
		}

		ar.Err <- nil
	}

	if err == nil {
		err = orpc.ErrServiceClosed
	}

	for ar := range reqC {
		ar.Err <- err
	}
}

var asyncResultPool sync.Pool

func AcquireAsyncResult() *AsyncResult {
	v := asyncResultPool.Get()
	if v == nil {
		return &AsyncResult{}
	}
	return v.(*AsyncResult)
}

func ReleaseAsyncResult(ar *AsyncResult) {
	ar.Method = 0
	ar.ReqKey = nil
	ar.ReqValue = nil

	ar.RespValue = nil

	ar.Err = nil

	asyncResultPool.Put(ar)
}
