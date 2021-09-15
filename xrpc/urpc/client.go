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

	// Size of send buffer per each underlying connection in bytes.
	// Default value is DefaultClientSendBufferSize.
	SendBufferSize int

	// Size of recv buffer per each underlying connection in bytes.
	// Default value is DefaultClientRecvBufferSize.
	RecvBufferSize int

	// Delay between request flushes.
	//
	// Negative values lead to immediate requests' sending to the server
	// without their buffering. This minimizes rpc latency at the cost
	// of higher CPU and network usage.
	//
	// Default value is DefaultFlushDelay.
	FlushDelay time.Duration
	NoReqSleep time.Duration

	// The client calls this callback when it needs new connection
	// to the server.
	// The client passes Client.Addr into Dial().
	//
	// By default it returns UNIX connections established to the Client.Addr.
	Dial DialFunc

	// CloseWait is the wait duration for Stop.
	CloseWait time.Duration

	nextConn  uint64
	reqQueues []*limitring.Ring
	connPool  []net.Conn

	requestsChan chan *asyncResult

	wg sync.WaitGroup
}

var _client xrpc.Client = new(Client)

// asyncResult is a result returned from Client.callAsync().
type asyncResult struct {
	method   uint8
	reqKey   []byte
	reqValue []byte

	respValue []byte

	err chan error
}

const (
	DefaultDB = uint32(1)
	// DefaultClientSendBufferSize is the default size for Client send buffers.
	DefaultClientSendBufferSize = 64 * 1024

	// DefaultClientRecvBufferSize is the default size for Client receive buffers.
	DefaultClientRecvBufferSize = 64 * 1024

	// DefaultClientConns is the default connection numbers for Client.
	DefaultClientConns = 16

	DefaultCloseWait = 3 * time.Second
)

// Start starts rpc client. Establishes connection to the server on Client.Addr.
func (c *Client) Start() error {

	if atomic.LoadInt64(&c.isRunning) == 1 {
		xlog.Debug("urpc client already started")
		return nil
	}

	config.Adjust(&c.DB, DefaultDB)
	config.Adjust(&c.PendingRequests, DefaultPendingMessages)
	config.Adjust(&c.SendBufferSize, DefaultClientSendBufferSize)
	config.Adjust(&c.RecvBufferSize, DefaultClientRecvBufferSize)
	config.Adjust(&c.Conns, DefaultClientConns)
	config.Adjust(&c.CloseWait, DefaultCloseWait)
	config.Adjust(&c.NoReqSleep, DefaultNoReqSleep)

	c.PendingRequests = xmath.NextPower2(c.PendingRequests)

	// Start write index at the value before 0
	// to allow the first conn to use AddUint64
	// and still have a beginning index of 0
	c.nextConn = ^c.nextConn
	c.reqQueues = make([]*limitring.Ring, c.Conns)
	for i := range c.reqQueues {
		c.reqQueues[i] = limitring.New(c.PendingRequests)
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

	for i := uint64(0); i < c.Conns; i++ {
		c.wg.Add(1)
		go c.handle(int(i))
	}

	atomic.StoreInt64(&c.isRunning, 1)
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

	var ar *asyncResult
	var err error
	if ar, err = c.callAsync(method, key, value); err != nil {
		return nil, nil, err
	}

	err = <-ar.err
	if err != nil {
		releaseAsyncResult(ar)
		return nil, nil, err
	}
	if ar.respValue != nil {
		v := ar.respValue
		releaseAsyncResult(ar)
		return v, PoolBytesCloser{v}, nil
	}

	return nil, nil, nil
}

func (c *Client) callAsync(method uint8, key, value []byte) (ar *asyncResult, err error) {

	if method != setMethod && method != getMethod {
		return nil, orpc.ErrNotImplemented
	}

	ar = acquireAsyncResult()

	ar.method = method
	ar.reqKey = key
	ar.err = make(chan error)

	if method == setMethod || method == setBatchMethod {
		ar.reqValue = value
	}

	err = c.dispatch(ar)
	if err != nil {
		releaseAsyncResult(ar)
		return nil, err // Queue is full.
	}
	return ar, nil
}

func (c *Client) handle(connIdx int) {
	defer c.wg.Done()

	q := c.reqQueues[connIdx]
	conn := c.connPool[connIdx]

	msg := new(msgBytes)
	reqH := new(reqHeader)
	respH := new(respHeader)

	respHBuf := make([]byte, respHeaderSize)

	for {
		if atomic.LoadInt64(&c.isRunning) != 1 {
			return
		}

		d, ok := q.Pop()
		if !ok {
			time.Sleep(c.NoReqSleep)
			continue
		}

		ar := (*asyncResult)(d)
		reqH.method = ar.method
		reqH.keySize = uint16(len(ar.reqKey))
		reqH.dbID = c.DB
		if ar.reqValue != nil {
			reqH.valueSize = uint32(len(ar.reqValue))
		} else {
			reqH.valueSize = 0
		}
		msg.header = reqH
		msg.key = ar.reqKey
		msg.value = ar.reqValue

		err := encodeToConn(conn, msg, true)
		if err != nil { // I don't think re-connect to a UDS is a good idea. Just return error to user.
			ar.err <- err
			xlog.Errorf("failed to send request to: %s: %s", c.Addr, err)
			resetMsg(msg)
			continue
		}

		resetMsg(msg)

		_, err = readAtLeast(conn, respHBuf, respHeaderSize)
		if err != nil {
			ar.err <- err
			xlog.Errorf("failed to read request header from %s: %s", conn.RemoteAddr().String(), err)
			continue
		}

		_ = respH.decode(respHBuf)

		errno := respH.errno
		if errno != 0 { // Ignore response if any error. And the response must be nil.
			ar.err <- orpc.Errno(errno).ToErr()
			continue
		}

		n := respH.bodySize
		if n == 0 {
			ar.err <- nil
			continue
		}

		if n != 0 {
			ar.respValue = xbytes.GetBytes(int(n))
			_, err = readAtLeast(conn, ar.respValue, int(n))
			if err != nil { // If failed to read body, the next read header will be failed too, so just return.
				xbytes.PutBytes(ar.respValue)
				xlog.Errorf("failed to read request body from %s: %s", conn.RemoteAddr().String(), err)
				ar.err <- err
				continue
			}
		}

		ar.err <- nil
	}
}

func resetMsg(msg *msgBytes) {
	msg.header = nil
	msg.key = nil
	msg.value = nil
}

// dispatch requests to connections by load points, the lower point the higher priority.
// TODO in present, we'll just use round robin to pick up connection.
// For zMatrix, every request's size is small, it's okay to regard all requests will bring the same load.
func (c *Client) dispatch(ar *asyncResult) error {

	// Actually I don't like %, but it's not a nice thing to set Conns to power of 2.
	idx := atomic.AddUint64(&c.nextConn, 1) % c.Conns

	return c.reqQueues[idx].Push(unsafe.Pointer(ar))
}

var asyncResultPool sync.Pool

func acquireAsyncResult() *asyncResult {
	v := asyncResultPool.Get()
	if v == nil {
		return &asyncResult{}
	}
	return v.(*asyncResult)
}

func releaseAsyncResult(ar *asyncResult) {
	ar.method = 0
	ar.reqKey = nil
	ar.reqValue = nil

	ar.respValue = nil
	ar.err = nil

	asyncResultPool.Put(ar)
}
