from ctypes import *

zmatrixlib = cdll.LoadLibrary('../binding/zmc.so')
init_client = zmatrixlib.init_client
zmc_set = zmatrixlib.set
zmc_get = zmatrixlib.get
init_client.argtypes = [c_char_p]
zmc_set.argtypes = [c_uint, c_char_p, c_char_p, c_uint, c_uint]
zmc_get.argtypes = [c_uint, c_char_p, c_uint, c_char_p,  POINTER(c_uint)]
init_client("/home/xxx/zmatrix.uds".encode('utf-8'))
zmatrixlib.set(1, "hello".encode('utf-8'), 5, "world".encode('utf-8'), 5)
val = "mybuf"
sizeval = c_int(0)
zmatrixlib.get(1, "hello".encode('utf-8'), 5, c_char_p(val.encode('utf-8')), byref(sizeval))

print("value_size = " + str(sizeval.value))
print("get key: hello, got value: " + str(val))
