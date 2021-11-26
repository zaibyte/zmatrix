package main

import "C"

import (
	"errors"
	"time"
	"unsafe"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zmatrix/pkg/xrpc"
	"g.tesamc.com/IT/zmatrix/pkg/xrpc/urpc"
)

var client xrpc.Client

//export init_client
func init_client(addr *C.char) C.int {
	client = urpc.NewClient(C.GoString(addr))
	err := client.Start()
	errno := orpc.ErrToErrno(err)
	return C.int(errno)
}

//export stop
func stop() {
	client.Stop(nil)
}

// set key, value to certain db.
// If db not found, will be created automatically.
//
// If meet ErrTooManyRequests, it'll keep retry every 3 seconds until ok or other error happens.

//export set
func set(db C.uint, key, value *C.char, kSize, vSize C.uint) C.int {

	k := charToBytes(key, int(kSize))
	v := charToBytes(value, int(vSize))

	errno := uint16(0)
	var err error
	for {
		err = client.Set(uint32(db), k, v)
		if err != nil {
			if !errors.Is(err, orpc.ErrTooManyRequests) {
				break
			} else {
				time.Sleep(3 * time.Second)
			}
		} else {
			break
		}
	}

	errno = uint16(orpc.ErrToErrno(err))
	return C.int(errno)
}

// get key-value from db.
// value is bytes buffer passed by invoker,
// get will return val size by update vSize pointer.

//export get
func get(db C.uint, key *C.char, kSize C.uint, value *C.char, vSize *C.uint) (errno C.int) {

	k := charToBytes(key, int(kSize))

	val, closer, err := client.Get(uint32(db), k)
	if err != nil {
		return C.int(uint16(orpc.ErrToErrno(err)))
	}
	defer closer.Close()

	v := charToBytes(value, len(val))
	copy(v, val)

	*vSize = C.uint(len(val))

	return 0
}

//export setBatch
func setBatch(db C.uint, keys, values []*C.char, keySizes, valueSizes []C.int) C.int {

	if len(keys) != len(values) {
		return C.int(orpc.ErrToErrno(orpc.ErrBadRequest))
	}
	if len(keySizes) != len(keys) {
		return C.int(orpc.ErrToErrno(orpc.ErrBadRequest))
	}
	if len(valueSizes) != len(keys) {
		return C.int(orpc.ErrToErrno(orpc.ErrBadRequest))
	}

	ks := make([][]byte, len(keys))
	vs := make([][]byte, len(values))

	for i := range ks {
		ks[i] = charToBytes(keys[i], int(keySizes[i]))
		vs[i] = charToBytes(values[i], int(valueSizes[i]))
	}

	errno := uint16(0)
	var err error
	for {
		err = client.SetBatch(uint32(db), ks, vs)
		if err != nil {
			if !errors.Is(err, orpc.ErrTooManyRequests) {
				break
			} else {
				time.Sleep(3 * time.Second)
			}
		} else {
			break
		}
	}

	errno = uint16(orpc.ErrToErrno(err))
	return C.int(errno)
}

// remove removes database entirely.

//export remove
func remove(db C.uint) C.int {

	return C.int(uint16(orpc.ErrToErrno(client.Remove(uint32(db)))))
}

// seal seals database.

//export seal
func seal(db C.uint) C.int {

	return C.int(uint16(orpc.ErrToErrno(client.Seal(uint32(db)))))
}

func charToBytes(src *C.char, sz int) []byte {
	return C.GoBytes(unsafe.Pointer(src), C.int(sz))
}

func main() {

}
