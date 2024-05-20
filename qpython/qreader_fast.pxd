



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
    cpdef skip(self, int offset)
    cpdef wrap(self, bytes data)
    cpdef bytes raw(self, int offset)
    cpdef get(self, str fmt)
    cpdef get_byte(self)
    cpdef int get_int(self)
    cpdef get_uint(self)
    cpdef get_long(self)
    cpdef get_symbol(self)
    cpdef get_symbols(self, int count)


cdef class QReader:
    cdef object _stream
    cdef BytesBuffer _buffer
    cdef str _encoding
    cdef dict _reader_map
    cdef bint _is_native
    cdef object _options
    cpdef _read_object(self)
    cpdef _read_error(self, int qtype)
    cpdef _read_string(self, int qtype)
    cpdef _read_symbol(self, int qtype)
    cpdef _read_char(self, int qtype)
    cpdef _read_guid(self, int qtype)
    cpdef _read_atom(self, int qtype)
    cpdef _read_temporal(self, int qtype)
    cpdef _read_list(self, int qtype)
    cpdef _read_dictionary(self, int qtype)
    cpdef _read_table(self, int qtype)
    cpdef _read_general_list(self, int qtype)
    cpdef _read_function(self, int qtype)
    cpdef _read_lambda(self, int qtype)
    cpdef _read_function_composition(self, int qtype)
    cpdef _read_adverb_function(self, int qtype)
    cpdef _read_projection(self, int qtype)
    cpdef bytes _read_bytes(self, int length)
