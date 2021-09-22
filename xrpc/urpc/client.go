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

	"g.tesamc.com/IT/zaipkg/xtime"

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

	DB   uint32 // One Client could only connect to one Database.
	Addr string // Server's address.

	// The number of concurrent connections the client should establish
	// to the sever.
	// Default is DefaultClientConns.
	Conns uint64

	// The maximum number of pending requests in the queue.
	//
	// The number of pending requests should exceed the expected number
	// of concurrent goroutines calling client's methods.
	// Otherwise, a lot of orpc.ErrRequestQueueOverflow errors may appear.
	//
	// Default is DefaultPendingMessages.
	PendingRequests uint64

	// The client calls this callback when it needs new connection
	// to the server.
	// The client passes Client.Addr into Dial().
	//
	// By default, it returns UNIX connections established to the Client.Addr.
	Dial DialFunc

	// Size of send buffer per each underlying connection in bytes.
	// Default value is DefaultClientSendBufferSize.
	SendBufferSize int

	// Size of recv buffer per each underlying connection in bytes.
	// Default value is DefaultClientRecvBufferSize.
	RecvBufferSize int

	// Each conn will have its own request chan for reducing conviction of chan.
	nextConn     uint64
	connPool     []net.Conn
	requestsChan []chan *asyncResult

	wg       sync.WaitGroup
	stopChan chan struct{}
}

var _ xrpc.Client = new(Client)

const (
	DefaultDB = uint32(1)

	// DefaultClientSendBufferSize is the default size for Client send buffers.
	DefaultClientSendBufferSize = 64 * 1024

	// DefaultClientRecvBufferSize is the default size for Client receive buffers.
	DefaultClientRecvBufferSize = 64 * 1024

	// DefaultClientConns is the default connection numbers for Client.
	// 4 conns is enough for 0.8 million QPS.
	DefaultClientConns = uint64(4)
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
	config.Adjust(&c.RecvBufferSize, DefaultClientRecvBufferSize)
	config.Adjust(&c.SendBufferSize, DefaultClientSendBufferSize)

	c.PendingRequests = xmath.NextPower2(c.PendingRequests)

	// Start write index at the value before 0
	// to allow the first conn to use AddUint64
	// and still have a beginning index of 0
	c.nextConn = ^c.nextConn
	c.requestsChan = make([]chan *asyncResult, c.Conns)
	for i := range c.requestsChan {
		c.requestsChan[i] = make(chan *asyncResult, c.PendingRequests/c.Conns)
	}
	c.connPool = make([]net.Conn, c.Conns)

	if c.Dial == nil {
		c.Dial = defaultDial
	}

	for i := uint64(0); i < c.Conns; i++ { // There is no reason that failed to establish UDS conn except serious issue.
		conn, err := c.Dial(c.Addr)
		if err != nil {
			c.closeConn()
			return err
		}

		c.connPool[i] = conn
	}

	c.stopChan = make(chan struct{})

	atomic.StoreInt64(&c.isRunning, 1)

	for i := uint64(0); i < c.Conns; i++ {
		c.wg.Add(1)
		go c.handleConn(int(i))
	}

	return nil
}

func (c *Client) closeConn() {

	for _, co := range c.connPool {
		if co != nil {
			_ = co.Close()
		}
	}
}

func (c *Client) Stop(err error) {
	if !atomic.CompareAndSwapInt64(&c.isRunning, 1, 0) {
		return
	}

	close(c.stopChan)

	c.closeConn()

	c.wg.Wait()

	if err == nil {
		err = orpc.ErrServiceClosed
	}

	for _, reqC := range c.requestsChan {
		close(reqC)
		for ar := range reqC {
			select {
			case ar.err <- err:
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
		return v, xbytes.PoolBytesCloser{P: v}, nil
	}

	return nil, nil, nil
}

func (c *Client) callAsync(method uint8, key, value []byte) (ar *asyncResult, err error) {

	ar = acquireAsyncResult()
	ar.method = method
	ar.reqKey = key
	ar.reqValue = value
	ar.err = make(chan error)

	c.nextConn++
	idx := c.nextConn % c.Conns

	reqC := c.requestsChan[idx]

	select {
	case reqC <- ar:
		return ar, nil
	default:
		// Try substituting the oldest async request by the new one
		// on requests' queue overflow.
		// This increases the chances for new request to succeed
		// without timeout.
		select {
		case ar2 := <-reqC:
			ar2.err <- orpc.ErrRequestQueueOverflow
		default:
		}

		// After pop, try to put again.
		select {
		case reqC <- ar:
			return ar, nil
		default:
			// RequestsChan is filled, release it since m wasn't exposed to the caller yet.
			releaseAsyncResult(ar)
			return nil, orpc.ErrRequestQueueOverflow
		}
	}
}

func (c *Client) handleConn(idx int) {

	defer c.wg.Done()

	conn := c.connPool[idx]
	reqChan := c.requestsChan[idx]

	stopChan := make(chan struct{})

	pendingRequests := make(map[uint64]*asyncResult, c.PendingRequests)
	var pendingRequestsLock sync.Mutex // Only two goroutine here, map with mutex is faster than sync.Map.

	writerDone := make(chan error, 1)
	go c.writeWorker(conn, reqChan, pendingRequests, &pendingRequestsLock, stopChan, writerDone)

	readerDone := make(chan error, 1)
	go c.readWorker(conn, pendingRequests, &pendingRequestsLock, readerDone)

	var err error
	select {
	case err = <-writerDone:
		close(stopChan)
		_ = conn.Close()
		<-readerDone
	case err = <-readerDone:
		close(stopChan)
		_ = conn.Close()
		<-writerDone
	case <-c.stopChan:
		close(stopChan)
		_ = conn.Close()
		<-readerDone
		<-writerDone
	}

	for _, ar := range pendingRequests {
		select {
		case ar.err <- err:
		default: // Avoiding blocking.
		}
	}
}

func (c *Client) writeWorker(conn net.Conn, reqC chan *asyncResult,
	pendingRequests map[uint64]*asyncResult, pendingRequestsLock *sync.Mutex,
	stopChan <-chan struct{}, done chan<- error) {

	var err error
	defer func() { done <- err }()

	var msgID uint64 = 1

	reqH := new(reqHeader)
	headerBuf := make([]byte, reqHeaderSize)
	enc := newEncoder(conn, c.SendBufferSize)
	msg := new(msgBytes)

	t := time.NewTimer(-1)
	var flushChan <-chan time.Time

	for {
		msgID++

		var ar *asyncResult

		select {
		case ar = <-reqC:
		default:
			// Give the last chance for ready goroutines filling c.requestsChan :)
			runtime.Gosched()

			select {
			case <-stopChan:
				return
			case ar = <-reqC:
			case <-flushChan:
				if err = enc.flush(); err != nil {
					xlog.Error(err.Error())
					return
				}
				flushChan = nil
				continue
			}
		}

		if flushChan == nil {
			flushChan = xtime.GetTimerEvent(t, -1)
		}

		pendingRequestsLock.Lock()
		pendingRequests[msgID] = ar
		pendingRequestsLock.Unlock()

		reqH.method = ar.method
		reqH.msgID = msgID
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

		err = enc.encodeNoFlush(msg, headerBuf)
		if err != nil { // I don't think re-connect to a UDS is a good idea. Just return error to user.
			ar.err <- err
			break
		}

		msg.reset()
		reqH.reset()
	}
}

func (c *Client) readWorker(r net.Conn, pendingRequests map[uint64]*asyncResult, pendingRequestsLock *sync.Mutex,
	done chan<- error) {

	var err error

	defer func() { done <- err }()

	dec := newDecoder(r, c.RecvBufferSize)
	rh := new(respHeader)
	headerBuf := make([]byte, respHeaderSize)
	for {

		err = dec.decodeHeader(headerBuf, rh)
		if err != nil {
			xlog.Errorf("failed to read request header from %s: %s", r.RemoteAddr().String(), err)
			return
		}

		msgID := rh.msgID

		pendingRequestsLock.Lock()
		ar, ok := pendingRequests[msgID]
		if ok {
			delete(pendingRequests, msgID)
		}
		pendingRequestsLock.Unlock()

		if !ok {
			xlog.Errorf("unexpected msgID: %d obtained from: %s", msgID, c.Addr)
			err = orpc.ErrInternalServer
			return
		}

		errno := rh.errno
		if errno != 0 { // Ignore response if any error. And the response must be nil.
			ar.err <- orpc.Errno(errno).ToErr()
			continue
		}

		n := rh.bodySize
		if n == 0 {
			ar.err <- nil
			continue
		}

		if n != 0 {
			ar.respValue = xbytes.GetBytes(int(n))
			err = dec.decodeBody(ar.respValue)
			if err != nil { // If failed to read body, the next read header will be failed too, so just return.
				xlog.Errorf("failed to read request body from %s: %s", r.RemoteAddr().String(), err)
				ar.err <- err
				xbytes.PutBytes(ar.respValue)
				return
			}
		}

		ar.err <- nil

		rh.reset()
	}
}

// asyncResult is a result returned from Client.callAsync().
type asyncResult struct {
	method   uint8
	reqKey   []byte
	reqValue []byte

	respValue []byte

	err chan error
}

var asyncRespPool sync.Pool

func acquireAsyncResult() *asyncResult {
	v := asyncRespPool.Get()
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

	asyncRespPool.Put(ar)
}
