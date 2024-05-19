



cdef class QMessage:
    cdef object _data
    cdef object _type
    cdef object _size
    cdef object _compression_mode


cdef class BytesBuffer:
    cdef str _endianness
    cdef bytes _data
    cdef int _position
    cdef int _size


cdef class QReader:
    cdef object _stream
    cdef BytesBuffer _buffer
    cdef str _encoding
    cdef dict _reader_map
