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
	"net"

	"g.tesamc.com/IT/zmatrix/xrpc"
)

// NewClient creates a client connecting UNIX
// to the server listening to the given addr.
//
// The returned client must be started after optional settings' adjustment.
//
// The corresponding server must be created with NewServer().
func NewClient(addr string, db uint32) *Client {
	c := &Client{
		DB:   db,
		Addr: addr,
		Dial: func(addr string) (conn net.Conn, err error) {
			return defaultDial(addr)
		},
	}

	return c
}

// DialFunc is a function intended for setting to Client.Dial.
type DialFunc func(addr string) (conn net.Conn, err error)

func defaultDial(addr string) (conn net.Conn, err error) {
	return unixDial(addr)
}

func unixDial(addr string) (conn net.Conn, err error) {
	c, err := net.Dial("unix", addr)
	if err != nil {
		return nil, err
	}
	return c, err
}

// NewServer creates a server listening UNIX connections
// on the given addr and processing incoming requests
// with the given Router.
//
// The returned server must be started after optional settings' adjustment.
//
// The corresponding client must be created with NewClient().
func NewServer(addr string, h xrpc.ServerHandler) *Server {
	s := &Server{
		Addr: addr,
		Listener: &netListener{
			F: func(addr string) (net.Listener, error) {
				return net.Listen("unix", addr)
			},
		},
		Handler: h,
		// Sacrifice the number of Write() calls to the smallest
		// possible latency, since it has higher priority in local IPC.
		FlushDelay: -1,
	}

	return s
}

type netListener struct {
	F func(addr string) (net.Listener, error)
	L net.Listener
}

func (ln *netListener) Init(addr string) (err error) {
	ln.L, err = ln.F(addr)
	return
}

func (ln *netListener) ListenAddr() net.Addr {
	if ln.L != nil {
		return ln.L.Addr()
	}
	return nil
}

func (ln *netListener) Accept() (conn net.Conn, err error) {
	c, err := ln.L.Accept()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (ln *netListener) Close() error {
	return ln.L.Close()
}

// Listener is an interface for custom listeners intended for the Server.
type Listener interface {
	// Init is called on server start.
	//
	// addr contains the address set at Server.Addr.
	Init(addr string) error

	// Accept must return incoming connections from clients.
	Accept() (conn net.Conn, err error)

	// Close closes the Listener.
	// All pending calls to Accept() must immediately return errors after
	// Close is called.
	// All subsequent calls to Accept() must immediately return error.
	Close() error

	// ListenAddr returns the listener's network address.
	ListenAddr() net.Addr
}
