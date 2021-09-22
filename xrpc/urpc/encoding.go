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

package urpc

import (
	"bufio"
	"io"
	"net"

	"g.tesamc.com/IT/zaipkg/xbytes"
)

type decoder struct {
	br *bufio.Reader
}

func newDecoder(conn net.Conn, bufsize int) *decoder {
	return &decoder{br: bufio.NewReaderSize(conn, bufsize)}
}

func (d *decoder) decodeHeader(buf []byte, h header) error {

	_, err := io.ReadFull(d.br, buf)
	if err != nil {
		return err
	}
	return h.decode(buf)
}

func (d *decoder) decodeBody(buf []byte) error {
	_, err := io.ReadFull(d.br, buf)
	return err
}

type encoder struct {
	bw *bufio.Writer
}

func newEncoder(conn net.Conn, bufsize int) *encoder {
	return &encoder{bw: bufio.NewWriterSize(conn, bufsize)}
}

type msgBytes struct {
	header header
	key    []byte
	value  []byte
}

func (m *msgBytes) reset() {
	m.header = nil
	m.key = nil
	m.value = nil
}

func (e *encoder) encode(msg *msgBytes, headerBuf []byte) error {
	var hbuf []byte
	_, ok := msg.header.(*reqHeader)
	if ok {
		hbuf = headerBuf[:reqHeaderSize]
	} else {
		hbuf = headerBuf[:respHeaderSize]
	}
	_ = msg.header.encode(hbuf)
	_, err := e.bw.Write(hbuf)
	if err != nil {
		return err
	}

	if msg.key != nil {
		_, err = e.bw.Write(msg.key)
		if err != nil {
			return err
		}
	}

	if msg.value != nil {
		_, err = e.bw.Write(msg.value)
		if err != nil {
			return err
		}
	}

	return e.bw.Flush()
}

// encodeBytesPool encodes msg which value is get from xbytes pool.
func (e *encoder) encodeBytesPool(msg *msgBytes, headerBuf []byte) error {
	var hbuf []byte
	_, ok := msg.header.(*reqHeader)
	if ok {
		hbuf = headerBuf[:reqHeaderSize]
	} else {
		hbuf = headerBuf[:respHeaderSize]
	}
	_ = msg.header.encode(hbuf)
	_, err := e.bw.Write(hbuf)
	if err != nil {
		return err
	}

	if msg.key != nil {
		_, err = e.bw.Write(msg.key)
		if err != nil {
			return err
		}
	}

	if msg.value != nil {
		_, err = e.bw.Write(msg.value)
		xbytes.PutBytes(msg.value)
		if err != nil {
			return err
		}
	}

	return err
}

func (e *encoder) flush() error {
	return e.bw.Flush()
}
