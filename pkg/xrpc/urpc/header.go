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
// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file contains code derived from Dragonboat.
// The main logic & codes are copied from Dragonboat.

package urpc

import (
	"encoding/binary"
)

const (
	setMethod      uint8 = 1
	getMethod      uint8 = 2
	setBatchMethod uint8 = 3
	removeMethod   uint8 = 4
	sealMethod     uint8 = 5
)

type header interface {
	encode(b []byte) []byte
	decode(b []byte) error
	getValueSize() uint32
}

const reqHeaderSize = 19

// reqHeader is the header for request.
type reqHeader struct {
	method    uint8  // [0, 1)
	msgID     uint64 // [1, 9)
	valueSize uint32 // [9, 13)
	keySize   uint16 // [13, 15)
	dbID      uint32 // [15, 19)
}

func (h *reqHeader) reset() {
	h.method = 0
	h.msgID = 0
	h.valueSize = 0
	h.keySize = 0
	h.dbID = 0
}

func (h *reqHeader) encode(buf []byte) []byte {
	if len(buf) < reqHeaderSize {
		panic("input buf too small for reqHeader")
	}
	buf[0] = h.method
	binary.BigEndian.PutUint64(buf[1:9], h.msgID)
	binary.BigEndian.PutUint32(buf[9:13], h.valueSize)
	binary.BigEndian.PutUint16(buf[13:15], h.keySize)
	binary.BigEndian.PutUint32(buf[15:19], h.dbID)
	return buf[:reqHeaderSize]
}

func (h *reqHeader) decode(buf []byte) error {
	if len(buf) < reqHeaderSize {
		panic("input buf too small for reqHeader")
	}

	h.method = buf[0]
	h.msgID = binary.BigEndian.Uint64(buf[1:9])
	h.valueSize = binary.BigEndian.Uint32(buf[9:13])
	h.keySize = binary.BigEndian.Uint16(buf[13:15])
	h.dbID = binary.BigEndian.Uint32(buf[15:19])

	return nil
}

func (h *reqHeader) getValueSize() uint32 {
	return h.valueSize
}

const respHeaderSize = 14

// respHeader is the header for response.
type respHeader struct {
	msgID    uint64 // [0, 8)
	errno    uint16 // [8, 10)
	bodySize uint32 // [10, 14)
}

func (h *respHeader) encode(buf []byte) []byte {
	if len(buf) < respHeaderSize {
		panic("input buf too small")
	}
	binary.BigEndian.PutUint64(buf[0:8], h.msgID)
	binary.BigEndian.PutUint16(buf[8:10], h.errno)
	binary.BigEndian.PutUint32(buf[10:14], h.bodySize)
	return buf[:respHeaderSize]
}

func (h *respHeader) decode(buf []byte) error {
	if len(buf) < respHeaderSize {
		panic("input buf too small")
	}

	h.msgID = binary.BigEndian.Uint64(buf[0:8])
	h.errno = binary.BigEndian.Uint16(buf[8:10])
	h.bodySize = binary.BigEndian.Uint32(buf[10:14])
	return nil
}

func (h *respHeader) getValueSize() uint32 {
	return h.bodySize
}

func (h *respHeader) reset() {
	h.msgID = 0
	h.errno = 0
	h.bodySize = 0
}
