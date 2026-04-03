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
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zaibyte/zaipkg/orpc"
	"github.com/zaibyte/zaipkg/xbytes"
	"github.com/zaibyte/zaipkg/xlog"
	"github.com/zaibyte/zaipkg/xtime"
	"github.com/zaibyte/zmatrix/pkg/xrpc"
)

// Server implements xrpc.Server.
//
// Default server settings are optimized for high load, so don't override
// them without valid reason.
type Server struct {
	// Address to listen to for incoming connections.
	// UNIX transport is used.
	Addr string

	// The maximum number of concurrent rpc calls the server may perform.
	// Default is DefaultConcurrency.
	Concurrency int

	// The maximum number of pending responses in the queue.
	// Default is DefaultPendingMessages.
	PendingResponses int

	// Size of send buffer per each underlying connection in bytes.
	// Default is DefaultBufferSize.
	SendBufferSize int

	// Size of recv buffer per each underlying connection in bytes.
	// Default is DefaultBufferSize.
	RecvBufferSize int

	// The server obtains new client connections via Listener.Accept().
	//
	// Override the Listener if you want custom underlying transport
	// and/or client authentication/authorization.
	// Don't forget overriding Client.Dial() callback accordingly.
	//
	// It returns UNIX connections accepted from Server.Addr.
	Listener Listener

	Handler xrpc.ServerHandler

	serverStopChan chan struct{}
	stopWg         sync.WaitGroup
}

const (
	// DefaultConcurrency is the default number of concurrent rpc calls
	// the server can process.
	DefaultConcurrency = 4096 // 4096 is enough to hold 1024 default clients, far away from enough for signle node.
	// DefaultServerSendBufferSize is the default size for Server send buffers.
	DefaultServerSendBufferSize = 64 * 1024
	// DefaultServerRecvBufferSize is the default size for Server receive buffers.
	DefaultServerRecvBufferSize = 64 * 1024
)

// Start starts rpc server.
func (s *Server) Start() error {

	if s.serverStopChan != nil {
		xlog.Panic("server is already running. Stop it before starting it again")
	}
	s.serverStopChan = make(chan struct{})

	if s.Handler == nil {
		xlog.Panic("no handler registered")
	}

	if s.Concurrency <= 0 {
		s.Concurrency = DefaultConcurrency
	}
	if s.PendingResponses <= 0 {
		s.PendingResponses = int(DefaultPendingMessages)
	}
	if s.SendBufferSize <= 0 {
		s.SendBufferSize = DefaultServerSendBufferSize
	}
	if s.RecvBufferSize <= 0 {
		s.RecvBufferSize = DefaultServerRecvBufferSize
	}

	if s.Listener == nil {
		s.Listener = &netListener{
			F: func(addr string) (net.Listener, error) {
				a, err := net.ResolveUnixAddr("unix", addr)
				if err != nil {
					return nil, err
				}
				return net.ListenUnix("unix", a)
			},
		}
	}
	if err := s.Listener.Init(s.Addr); err != nil {
		xlog.Errorf("cannot listen to: %s: %s", s.Addr, err.Error())
		return err
	}

	workersCh := make(chan struct{}, s.Concurrency)

	s.stopWg.Add(1)
	go s.handler(workersCh)
	return nil
}

// Stop stops rpc server. Stopped server can be started again.
func (s *Server) Stop(_ error) {
	if s.serverStopChan == nil {
		xlog.Panic("server must be started before stopping it")
	}
	close(s.serverStopChan)
	s.stopWg.Wait()
	s.serverStopChan = nil
}

// Serve starts rpc server and blocks until it is stopped.
func (s *Server) Serve() error {
	if err := s.Start(); err != nil {
		return err
	}
	s.stopWg.Wait()
	return nil
}

func (s *Server) handler(workersCh chan struct{}) {
	defer s.stopWg.Done()

	var conn net.Conn
	var err error
	var stopping atomic.Value

	for {
		acceptChan := make(chan struct{})
		go func() {
			if conn, err = s.Listener.Accept(); err != nil {
				xlog.Errorf("failed to accept: %s", err.Error())
				if stopping.Load() == nil {
					xlog.Errorf("cannot accept new connection: %s", err)
				}
			}
			close(acceptChan)
		}()

		select {
		case <-s.serverStopChan:
			stopping.Store(true)
			_ = s.Listener.Close()
			<-acceptChan
			return
		case <-acceptChan:
		}

		if err != nil {
			select {
			case <-s.serverStopChan:
				return
			case <-time.After(time.Second):
			}
			continue
		}

		s.stopWg.Add(1)
		go s.handleConnection(conn, workersCh)
	}
}

func (s *Server) handleConnection(conn net.Conn, workersCh chan struct{}) {
	defer s.stopWg.Done()

	responsesChan := make(chan *serverMessage, s.PendingResponses)
	stopChan := make(chan struct{})

	readerDone := make(chan struct{})
	go s.serverReader(conn, responsesChan, stopChan, readerDone, workersCh)

	writerDone := make(chan struct{})
	go s.serverWriter(conn, responsesChan, stopChan, writerDone)

	select {
	case <-readerDone:
		close(stopChan)
		_ = conn.Close()
		<-writerDone
	case <-writerDone:
		close(stopChan)
		_ = conn.Close()
		<-readerDone
	case <-s.serverStopChan:
		close(stopChan)
		_ = conn.Close()
		<-readerDone
		<-writerDone
	}
}

type serverMessage struct {
	method   uint8
	msgID    uint64
	reqKey   []byte
	reqValue []byte
	dbID     uint32

	resp       []byte
	respCloser io.Closer
	err        error
}

var serverMessagePool = &sync.Pool{
	New: func() interface{} {
		return &serverMessage{}
	},
}

func (s *serverMessage) reset() {
	s.method = 0
	s.msgID = 0
	s.reqKey = nil
	s.reqValue = nil
	s.dbID = 0

	s.resp = nil
	s.respCloser = nil
	s.err = nil
}

func (s *Server) serverReader(r net.Conn, responsesChan chan<- *serverMessage,
	stopChan <-chan struct{}, done chan<- struct{}, workersCh chan struct{}) {

	defer func() {
		if x := recover(); x != nil {
			stackTrace := make([]byte, 1<<20)
			n := runtime.Stack(stackTrace, false)
			xlog.Errorf("panic when reading data from client: %v\nStack trace: %s", x, stackTrace[:n])
		}
		close(done)
	}()

	dec := newDecoder(r, s.RecvBufferSize)
	rh := new(reqHeader)
	headerBuf := make([]byte, reqHeaderSize)

	for {
		err := dec.decodeHeader(headerBuf, rh)
		if err != nil {
			xlog.Errorf("failed to read request header from %s: %s", r.RemoteAddr().String(), err)
			return
		}

		m := serverMessagePool.Get().(*serverMessage)
		m.method = rh.method
		m.msgID = rh.msgID
		m.dbID = rh.dbID

		n := int(rh.keySize) + int(rh.valueSize) // At least has key or value.
		body := xbytes.GetBytes(n)
		err = dec.decodeBody(body)
		if err != nil {
			xlog.Errorf("failed to read request key & value from %s: %s", r.RemoteAddr().String(), err.Error())
			xbytes.PutBytes(body)
			m.reset()
			serverMessagePool.Put(m)
			return
		}
		if rh.keySize != 0 {
			m.reqKey = body[:rh.keySize]
		}
		if rh.valueSize != 0 {
			m.reqValue = body[rh.keySize:]
		}

		// Blocking until we have free worker.
		select {
		case workersCh <- struct{}{}:
		default:
			select {
			case workersCh <- struct{}{}:
			case <-stopChan:
				return
			}
		}

		// Haven read the request, handle request async, free the connection for the next request reading.
		go s.serveRequest(responsesChan, stopChan, m, workersCh)
	}
}

func (s *Server) serveRequest(responsesChan chan<- *serverMessage, stopChan <-chan struct{}, m *serverMessage, workersCh <-chan struct{}) {

	if m.err == nil {
		resp, closer, err := s.callHandlerWithRecover(m.method, m.dbID, m.reqKey, m.reqValue)
		m.resp = resp
		m.respCloser = closer
		m.err = err
		if err != nil {
			m.resp = nil
		}
	}

	// req bytes is got by xbytes pool, need put back.
	// If it has key, the bytes must be got start from it.
	// Otherwise, the bytes must be got start from value.
	if m.reqKey != nil {
		xbytes.PutBytes(m.reqKey)
	} else {
		xbytes.PutBytes(m.reqValue) // If key is nil, must be setBatch.
	}

	m.reqKey = nil
	m.reqValue = nil

	// Select hack for better performance.
	// See https://github.com/valyala/gorpc/pull/1 for details.
	select {
	case responsesChan <- m:
	default:
		select {
		case responsesChan <- m:
		case <-stopChan:
		}
	}

	<-workersCh
}

func (s *Server) callHandlerWithRecover(method uint8, dbID uint32, reqKey, reqValue []byte) (resp []byte, closer io.Closer, err error) {
	defer func() {
		if x := recover(); x != nil {
			stackTrace := make([]byte, 1<<20)
			n := runtime.Stack(stackTrace, false)
			err = fmt.Errorf("panic occured: %v\nStack trace: %s", x, stackTrace[:n])
			xlog.Error(err.Error())
		}
	}()

	if method == getMethod {
		return s.Handler.Get(dbID, reqKey)
	}

	switch method {
	case setMethod:
		err = s.Handler.Set(dbID, reqKey, reqValue)
		return nil, nil, err
	case setBatchMethod:
		keys, values := extraSetBatchReq(reqValue)
		err = s.Handler.SetBatch(dbID, keys, values)
		return nil, nil, err
	case removeMethod:
		err = s.Handler.Remove(dbID)
		return nil, nil, err
	case sealMethod:
		err = s.Handler.Seal(dbID)
		return nil, nil, err
	case getStateMethod:
		state, err := s.Handler.GetState(dbID)
		if err != nil {
			return nil, nil, err
		}
		resp = xbytes.GetBytes(4)
		binary.LittleEndian.PutUint32(resp, uint32(state))
		return resp, xbytes.PoolBytesCloser{P: resp}, nil
	default:
		return nil, nil, orpc.ErrNotImplemented
	}
}

func (s *Server) serverWriter(w net.Conn, responsesChan <-chan *serverMessage, stopChan <-chan struct{}, done chan<- struct{}) {
	defer func() { close(done) }()

	enc := newEncoder(w, s.SendBufferSize)
	msg := new(msgBytes)
	rh := new(respHeader)
	headerBuf := make([]byte, respHeaderSize) // reqHeaderSize is bigger than respHeaderSize.

	t := time.NewTimer(-1)
	var flushChan <-chan time.Time

	for {
		var m *serverMessage

		select {
		case m = <-responsesChan:
		default:
			// Give the last chance for ready goroutines filling responsesChan :)
			runtime.Gosched()

			select {
			case <-stopChan:
				return
			case m = <-responsesChan:
			case <-flushChan:
				if err := enc.flush(); err != nil {
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

		resp := m.resp
		rh.msgID = m.msgID

		if resp != nil {
			rh.bodySize = uint32(len(resp))
		} else {
			rh.bodySize = 0
		}
		rh.errno = uint16(orpc.ErrToErrno(m.err))
		msg.header = rh
		msg.value = resp

		respCloser := m.respCloser
		m.reset()
		serverMessagePool.Put(m)

		if err := enc.encodeNoFlush(msg, headerBuf); err != nil {

			xlog.Errorf("failed to send response to: %s: %s", w.RemoteAddr().String(), err)
			return
		}

		if respCloser != nil {
			_ = respCloser.Close()
		}

		msg.reset()
		rh.reset()
	}
}
