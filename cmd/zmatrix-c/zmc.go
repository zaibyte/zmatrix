package main

//#include <stdint.h>
import "C"

import (
	"io"
	"unsafe"

	"g.tesamc.com/IT/zaipkg/orpc"

	"g.tesamc.com/IT/zmatrix/pkg/xrpc"
	"g.tesamc.com/IT/zmatrix/pkg/xrpc/urpc"
)

var client xrpc.Client

//export init_client
func initClient(addr *C.char) {
	client = urpc.NewClient(C.GoString(addr))
}

//export exit
func exit() {
	client.Stop(nil)
}

// set key, value to certain db.
// If db not found, will be created automatically.
//export set
func set(db C.uint32_t, key, value *C.char, kSize, vSize C.uint32_t) C.uint16_t {

	k := charToBytes(key, kSize)
	v := charToBytes(value, vSize)
	err := client.Set(db, k, v)
	return orpc.ErrToErrno(err)
}

func get(db C.uint32_t, key []byte) (value []byte, closer io.Closer, errno C.uint16_t) {

}

func setBatch(db C.uint32_t, keys, values [][]byte) C.uint16_t {

}

// remove removes database entirely.
func remove(db C.uint32_t) C.uint16_t {

}

// seal seals database.
func seal(db C.uint32_t) C.uint16_t {

}

func charToBytes(src *C.char, sz int) []byte {
	return C.GoBytes(unsafe.Pointer(src), C.int(sz))
}

func main() {

}
