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

func (d *decoder) decodeBody(buf []byte, n int) error {
	_, err := readAtLeast(d.br, buf, n)
	return err
}

// readAtLeast reads from r into buf until it has read at least min bytes.
// It returns the number of bytes copied and an error if fewer bytes were read.
// The error is EOF only if no bytes were read.
// If an EOF happens after reading fewer than min bytes,
// readAtLeast returns ErrUnexpectedEOF.
// If min is greater than the length of buf, ReadAtLeast returns ErrShortBuffer.
// On return, n >= min if and only if Err == nil.
// If r returns an error having read at least min bytes, the error is dropped.
func readAtLeast(r io.Reader, buf []byte, min int) (n int, err error) {
	if len(buf) < min {
		return 0, io.ErrShortBuffer
	}
	for n < min && err == nil {
		var nn int
		nn, err = r.Read(buf[n:min])
		n += nn
	}

	if n >= min {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return
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
	return err
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

func encodeToConn(conn net.Conn, h header, key, value []byte, isReq bool) error {

	bufSize := 0
	headerSize := 0
	if isReq {
		headerSize = reqHeaderSize
	} else {
		headerSize = respHeaderSize
	}

	bufSize += headerSize
	bufSize += len(key)
	bufSize += len(value)

	buf := xbytes.GetBytes(bufSize)
	defer xbytes.PutBytes(buf)
	_ = h.encode(buf[:headerSize])
	copy(buf[headerSize:], key)
	copy(buf[headerSize+len(key):], value)

	_, err := conn.Write(buf)
	return err
}
