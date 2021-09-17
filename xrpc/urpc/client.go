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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtaci/gaio"

	"g.tesamc.com/IT/zaipkg/xmath"

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
	connPool     []net.Conn
	watchers     []*gaio.Watcher // Each connection has a watcher for breaking up one chan's limitation.
	requestsChan []chan *AsyncResult

	wg       sync.WaitGroup
	stopChan chan struct{}
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
	c.requestsChan = make([]chan *AsyncResult, c.Conns)
	for i := range c.requestsChan {
		c.requestsChan[i] = make(chan *AsyncResult, c.PendingRequests/2/c.Conns)
	}

	c.connPool = make([]net.Conn, c.Conns)
	c.watchers = make([]*gaio.Watcher, c.Conns)

	if c.Dial == nil {
		c.Dial = defaultDial
	}

	for i := uint64(0); i < c.Conns; i++ { // There is no reason that failed to establish UDS conn except serious issue.
		conn, err := c.Dial(c.Addr)
		if err != nil {
			c.closeConn()
			return err
		}
		watcher, err := gaio.NewWatcher()
		if err != nil {
			c.closeConn()
			return err
		}

		c.connPool[i] = conn
		c.watchers[i] = watcher
	}

	c.stopChan = make(chan struct{})

	atomic.StoreInt64(&c.isRunning, 1)

	for i := uint64(0); i < c.Conns; i++ {
		c.wg.Add(1)
		go c.watchEvents(int(i))
	}

	return nil
}

func (c *Client) closeConn() {

	for _, co := range c.connPool {
		if co != nil {
			_ = co.Close()
		}
	}

	for _, wa := range c.watchers {
		if wa != nil {
			_ = wa.Close()
		}
	}
}

func (c *Client) Stop(err error) {
	if !atomic.CompareAndSwapInt64(&c.isRunning, 1, 0) {
		return
	}

	close(c.stopChan)

	c.wg.Wait()

	c.closeConn()

	if err == nil {
		err = orpc.ErrServiceClosed
	}

	for _, reqC := range c.requestsChan {
		close(reqC)
		for ar := range reqC {
			select {
			case ar.Err <- err:
			default: // Avoiding blocking.
			}
		}
	}
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

	var ar *asyncResp
	var err error
	if ar, err = c.callAsync(method, key, value); err != nil {
		return nil, nil, err
	}

	err = <-ar.err
	if err != nil {
		releaseAsyncResp(ar)
		return nil, nil, err
	}
	if ar.resp != nil {
		v := ar.resp
		releaseAsyncResp(ar)
		return v, PoolBytesCloser{v}, nil
	}

	return nil, nil, nil
}

func (c *Client) callAsync(method uint8, key, value []byte) (ar *asyncResp, err error) {

	if method != setMethod && method != getMethod {
		return nil, orpc.ErrNotImplemented
	}

	ar = acquireAsyncResp()
	ar.err = make(chan error)

	buf := xbytes.GetBytes(reqHeaderSize + len(key) + len(value))
	defer xbytes.PutBytes(buf)

	c.nextConn++
	idx := c.nextConn % c.Conns

	w := c.watchers[idx]
	conn := c.connPool[idx]

	h := acquireReqHeader()
	defer releaseReqHeader(h)

	h.method = method
	h.dbID = c.DB
	h.keySize = uint16(len(key))
	h.valueSize = uint32(len(value))
	h.encode(buf[:reqHeaderSize])

	copy(buf[reqHeaderSize:], key)
	copy(buf[reqHeaderSize+len(key):], value)

	// TODO I'm not sure the lock inside w's cost.
	err = w.Write(ar, conn, buf)
	if err != nil {
		releaseAsyncResp(ar)
		return nil, err
	}
	return ar, nil
}

func (c *Client) isClosed() bool {

	return atomic.LoadInt64(&c.isRunning) == 0
}

func (c *Client) watchEvents(i int) {

	defer c.wg.Done()

	w := c.watchers[i]

	respH := new(respHeader)
	zeroTime := time.Time{}

	pending := make(map[uint64]*asyncResp)
	var msgID uint64

	for {

		if c.isClosed() {
			return
		}

		// loop wait for any IO events
		results, err := w.WaitIO()
		if err != nil {
			xlog.Errorf("client failed to wait io events: %s", err.Error()) // Socket closed in most case.
			return
		}

		for _, res := range results {
			switch res.Operation {
			case gaio.OpRead: // read from server side completion event
				ar := res.Context.(*asyncResp)
				if res.Error == nil {
					if ar.isHeader {
						_ = respH.decode(res.Buffer[:respHeaderSize])
						if respH.errno != 0 {
							ar.err <- orpc.Errno(respH.errno).ToErr()
							respH.reset()
							continue
						} else {
							if respH.bodySize == 0 {
								ar.err <- nil
								respH.reset()
								continue
							} else {
								pending[msgID] = ar

								ar.msgID = msgID
								ar.isHeader = false
								valueLen := int(respH.bodySize)
								value := xbytes.GetBytes(valueLen)
								ar.resp = value
								err = w.ReadFull(ar, res.Conn, value, zeroTime)
								if err != nil {
									xlog.Errorf("client failed to create read event: %s", err.Error())
									return
								}

								respH.reset()
								msgID++
							}
						}
					} else {
						par := pending[ar.msgID]
						par.resp = ar.resp
						par.err <- nil

						delete(pending, ar.msgID)
						continue
					}
				} else {
					ar.err <- xerrors.WithMessage(orpc.ErrConnection, res.Error.Error())
					continue
				}
			case gaio.OpWrite: // write to server completion event, try to read response.
				ar := res.Context.(*asyncResp)
				if res.Error == nil {
					// Passing context back to read for send message to client call.
					ar.isHeader = true
					err = w.ReadFull(ar, res.Conn, res.Buffer[:respHeaderSize], zeroTime)
					if err != nil {
						xlog.Errorf("client failed to create read event: %s", err.Error())
						return
					}
				} else {
					ar.err <- xerrors.WithMessage(orpc.ErrConnection, res.Error.Error())
					continue
				}
			}
		}
	}
}

func (c *Client) runReqWorker(i int) {

	defer c.wg.Done()

	conn := c.connPool[i]
	reqC := c.requestsChan[i]

	respH := new(respHeader)
	respHBuf := make([]byte, respHeaderSize)

	reqH := new(reqHeader)
	reqBuf := make([]byte, 64*1024)

	var err error
	for {
		if atomic.LoadInt64(&c.isRunning) != 1 {
			break
		}

		var ar *AsyncResult
		select {
		case ar = <-reqC:
		default:
			runtime.Gosched()
			select {
			case ar = <-reqC:
			case <-c.stopChan:
				break
			default:
				continue
			}
		}
		if ar == nil {
			continue
		}

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
}

// func (c *Client) writeWorker(idx int, pendingRequests map[uint64]*AsyncResult, pendingRequestsLock *sync.Mutex,
// 	stopChan <-chan struct{}, done chan<- error) {
//
// 	var err error
// 	defer func() { done <- err }()
//
// 	var msgID uint64 = 1
//
// 	reqC := c.requestsChan[idx]
// 	w := c.connPool[idx]
//
// 	reqH := new(reqHeader)
// 	reqBuf := make([]byte, 64*1024)
//
// 	for {
// 		msgID++
//
// 		var ar *AsyncResult
//
// 		select {
// 		case ar = <-reqC:
// 		default:
// 			// Give the last chance for ready goroutines filling c.requestsChan :)
// 			runtime.Gosched()
//
// 			select {
// 			case <-stopChan:
// 				return
// 			case ar = <-reqC:
// 			default:
// 				continue
// 			}
// 		}
//
// 		if ar == nil {
// 			continue
// 		}
//
// 		pendingRequestsLock.Lock()
// 		n := len(pendingRequests)
// 		pendingRequests[msgID] = ar
// 		pendingRequestsLock.Unlock()
//
// 		if n > int(10*c.PendingRequests) {
// 			xlog.Errorf("server: %s didn't return %d responses yet: closing connection", c.Addr, n)
// 			err = orpc.ErrConnection
// 			return
// 		}
//
// 		reqH.method = ar.Method
// 		reqH.keySize = uint16(len(ar.ReqKey))
// 		reqH.dbID = c.DB
// 		if ar.ReqValue != nil {
// 			reqH.valueSize = uint32(len(ar.ReqValue))
// 		} else {
// 			reqH.valueSize = 0
// 		}
//
// 		err = encodeToConn(w, reqH, ar.ReqKey, ar.ReqValue, reqBuf, true)
// 		if err != nil { // I don't think re-connect to a UDS is a good idea. Just return error to user.
// 			ar.Err <- err
// 			break
// 		}
//
// 	}
// }
//
// func (c *Client) readWorker(conn net.Conn) {
//
// }

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

type asyncResp struct {
	msgID    uint64
	isHeader bool

	resp []byte

	err chan error
}

var asyncRespPool sync.Pool

func acquireAsyncResp() *asyncResp {
	v := asyncRespPool.Get()
	if v == nil {
		return &asyncResp{}
	}
	return v.(*asyncResp)
}

func releaseAsyncResp(ar *asyncResp) {
	ar.msgID = 0
	ar.isHeader = false

	ar.resp = nil

	ar.err = nil

	asyncRespPool.Put(ar)
}

var reqHeaderPool sync.Pool

func acquireReqHeader() *reqHeader {
	v := reqHeaderPool.Get()
	if v == nil {
		return &reqHeader{}
	}
	return v.(*reqHeader)
}

func releaseReqHeader(h *reqHeader) {

	h.reset()

	reqHeaderPool.Put(h)
}
