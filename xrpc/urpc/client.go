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
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"g.tesamc.com/IT/zaipkg/xbytes"

	"g.tesamc.com/IT/zmatrix/xrpc"

	"g.tesamc.com/IT/zaipkg/config"

	"g.tesamc.com/IT/zaipkg/xtime"

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
	Conns int

	// The maximum number of pending requests in the queue.
	//
	// The number of pending requests should exceed the expected number
	// of concurrent goroutines calling client's methods.
	// Otherwise a lot of orpc.ErrRequestQueueOverflow errors may appear.
	//
	// Default is DefaultPendingMessages.
	PendingRequests int

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

	// The client calls this callback when it needs new connection
	// to the server.
	// The client passes Client.Addr into Dial().
	//
	// By default it returns UNIX connections established to the Client.Addr.
	Dial DialFunc

	// CloseWait is the wait duration for Stop.
	CloseWait time.Duration

	requestsChan chan *asyncResult

	stopChan chan struct{}
	stopWg   sync.WaitGroup
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

	if c.stopChan != nil {
		xlog.Panic("already started")
	}

	config.Adjust(&c.DB, DefaultDB)
	config.Adjust(&c.PendingRequests, DefaultPendingMessages)
	config.Adjust(&c.SendBufferSize, DefaultClientSendBufferSize)
	config.Adjust(&c.RecvBufferSize, DefaultClientRecvBufferSize)
	config.Adjust(&c.FlushDelay, DefaultFlushDelay)
	config.Adjust(&c.Conns, DefaultClientConns)
	config.Adjust(&c.CloseWait, DefaultCloseWait)

	c.requestsChan = make(chan *asyncResult, c.PendingRequests)
	c.stopChan = make(chan struct{})

	if c.Dial == nil {
		c.Dial = defaultDial
	}

	for i := 0; i < c.Conns; i++ {
		c.stopWg.Add(1)
		go c.clientHandler()
	}

	atomic.StoreInt64(&c.isRunning, 1)
	return nil
}

func (c *Client) Stop(err error) {
	if !atomic.CompareAndSwapInt64(&c.isRunning, 1, 0) {
		return
	}

	if c.stopChan == nil {
		xlog.Panic("client must be started before stopping it")
	}
	close(c.stopChan)

	c.stopWg.Wait()

	if err == nil {
		err = orpc.ErrServiceClosed
	}

	t := xtime.AcquireTimer(c.CloseWait)

	for {
		select {
		case r := <-c.requestsChan:
			r.err <- err
			continue
		case <-t.C:
			goto reset
		default:
			continue
		}
	}

reset:
	xtime.ReleaseTimer(t)

	c.stopChan = nil
}

func (c *Client) Set(key, value []byte) error {
	_, _, err := c.call(setMethod, key, value)
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

type PoolBytesCloser struct {
	p []byte
}

func (r PoolBytesCloser) Close() error {
	xbytes.PutBytes(r.p)
	return nil
}

func (c *Client) callAsync(method uint8, key, value []byte) (ar *asyncResult, err error) {

	if method != setMethod && method != getMethod {
		return nil, orpc.ErrNotImplemented
	}

	ar = acquireAsyncResult()

	ar.method = method
	ar.reqKey = key
	ar.err = make(chan error)

	if method == setMethod {
		ar.reqValue = value
	}

	select {
	case c.requestsChan <- ar:
		return ar, nil
	default:
		// Try substituting the oldest async request by the new one
		// on requests' queue overflow.
		// This increases the chances for new request to succeed
		// without timeout.
		select {
		case ar2 := <-c.requestsChan:
			ar2.err <- orpc.ErrRequestQueueOverflow
		default:
		}

		// After pop, try to put again.
		select {
		case c.requestsChan <- ar:
			return ar, nil
		default:
			// RequestsChan is filled, release it since m wasn't exposed to the caller yet.
			releaseAsyncResult(ar)
			return nil, orpc.ErrRequestQueueOverflow
		}
	}
}

func (c *Client) clientHandler() {
	defer c.stopWg.Done()

	var conn net.Conn
	var err error
	var stopping atomic.Value

	for {
		dialChan := make(chan struct{})
		go func() {
			if conn, err = c.Dial(c.Addr); err != nil {
				if stopping.Load() == nil {
					xlog.Errorf("cannot establish rpc connection to: %s: %s", c.Addr, err)
				}
			}
			close(dialChan)
		}()

		select {
		case <-c.stopChan:
			stopping.Store(true)
			<-dialChan
			return
		case <-dialChan:
		}

		if err != nil {
			select {
			case <-c.stopChan:
				return
			case <-time.After(300 * time.Millisecond): // After 300ms, try to dial again.
			}
			continue
		}
		c.clientHandleConnection(conn)

		select {
		case <-c.stopChan:
			return
		default:
		}
	}
}

func (c *Client) clientHandleConnection(conn net.Conn) {

	var err error

	stopChan := make(chan struct{})

	pendingRequests := make(map[uint64]*asyncResult, c.PendingRequests)
	var pendingRequestsLock sync.Mutex // Only two goroutine here, map with mutex is faster than sync.Map.

	writerDone := make(chan error, 1)
	go c.clientWriter(conn, pendingRequests, &pendingRequestsLock, stopChan, writerDone)

	readerDone := make(chan error, 1)
	go c.clientReader(conn, pendingRequests, &pendingRequestsLock, readerDone)

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

func (c *Client) clientWriter(w net.Conn, pendingRequests map[uint64]*asyncResult, pendingRequestsLock *sync.Mutex,
	stopChan <-chan struct{}, done chan<- error) {

	var err error
	defer func() { done <- err }()

	enc := newEncoder(w, c.SendBufferSize)
	msg := new(msgBytes)
	rh := new(reqHeader)

	t := time.NewTimer(c.FlushDelay)
	var flushChan <-chan time.Time

	var msgID uint64 = 1
	headerBuf := make([]byte, reqHeaderSize) // reqHeaderSize is bigger than respHeaderSize.

	for {
		msgID++

		var ar *asyncResult

		select {
		case ar = <-c.requestsChan:
		default:
			// Give the last chance for ready goroutines filling c.requestsChan :)
			runtime.Gosched()

			select {
			case <-stopChan:
				return
			case ar = <-c.requestsChan:
			case <-flushChan:
				if err = enc.flush(); err != nil {
					err = fmt.Errorf("client cannot requests to: %s: %s", c.Addr, err)
					return
				}
				flushChan = nil
				continue
			}
		}

		if flushChan == nil {
			flushChan = xtime.GetTimerEvent(t, c.FlushDelay)
		}

		pendingRequestsLock.Lock()
		n := len(pendingRequests)
		pendingRequests[msgID] = ar
		pendingRequestsLock.Unlock()

		if n > 10*c.PendingRequests {
			xlog.Errorf("server: %s didn't return %d responses yet: closing connection", c.Addr, n)
			err = orpc.ErrConnection
			return
		}

		rh.method = ar.method
		rh.msgID = msgID
		rh.keySize = uint16(len(ar.reqKey))
		rh.dbID = c.DB
		if ar.reqValue != nil {
			rh.valueSize = uint32(len(ar.reqValue))
		} else {
			rh.valueSize = 0
		}
		msg.header = rh
		msg.key = ar.reqKey
		msg.value = ar.reqValue

		if err = enc.encode(msg, headerBuf); err != nil {
			xlog.Errorf("failed to send request to: %s: %s", c.Addr, err)
			return
		}
		msg.header = nil
		msg.key = nil
		msg.value = nil
	}
}

func (c *Client) clientReader(r net.Conn, pendingRequests map[uint64]*asyncResult, pendingRequestsLock *sync.Mutex, done chan<- error) {
	var err error
	defer func() {
		if x := recover(); x != nil {
			if err == nil {
				stackTrace := make([]byte, 1<<20)
				n := runtime.Stack(stackTrace, false)
				xlog.Errorf("panic when reading data from server: %s: %v\nStack trace: %s", r.RemoteAddr().String(), x, stackTrace[:n])
			}
		}

		done <- err
	}()

	dec := newDecoder(r, c.RecvBufferSize)
	rh := new(respHeader)
	headerBuf := make([]byte, respHeaderSize)
	for {

		err = dec.decodeHeader(headerBuf, rh)
		if err != nil {
			if err == orpc.ErrTimeout {
				continue // Keeping trying to read request header.
			}
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
			err = dec.decodeBody(ar.respValue, int(n))
			if err != nil { // If failed to read body, the next read header will be failed too, so just return.
				xbytes.PutBytes(ar.respValue)
				xlog.Errorf("failed to read request body from %s: %s", r.RemoteAddr().String(), err)
				ar.err <- err
				return
			}
		}

		ar.err <- nil
	}
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
