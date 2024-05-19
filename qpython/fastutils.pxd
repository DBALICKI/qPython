import numpy

cimport numpy

ctypedef numpy.int64_t DTYPE_t
ctypedef numpy.uint8_t DTYPE8_t

cpdef uncompress(numpy.ndarray[DTYPE8_t, ndim=1] data, DTYPE_t uncompressed_size)
