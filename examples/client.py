from ctypes import *

zmatrixlib = cdll.LoadLibrary('../binding/zmc.so')
zmc_init_client = zmatrixlib.zmc_init_client
zmc_stop_client = zmatrixlib.zmc_stop
zmc_set = zmatrixlib.zmc_set
zmc_get = zmatrixlib.zmc_get
zmc_init_client.argtypes = [c_char_p]
zmc_set.argtypes = [c_uint, c_char_p, c_char_p, c_uint, c_uint]
zmc_get.argtypes = [c_uint, c_char_p, c_uint, POINTER(c_char),  POINTER(c_uint)]
zmc_init_client("/home/xxx/zmatrix.uds".encode('utf-8'))
zmc_set(c_uint(1), c_char_p("hello".encode('utf-8')), c_char_p("world".encode('utf-8')), c_uint(5), c_uint(5))
val = (c_char * 20)()
sizeval = c_uint(0)
zmc_get(c_uint(1), c_char_p("hello".encode('utf-8')), c_uint(5), val, byref(sizeval))

print("value_size = " + str(sizeval.value))
print("get key: hello, got value: " + str(val[:sizeval.value]))

zmc_stop_client()
