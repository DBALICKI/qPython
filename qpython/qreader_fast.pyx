import struct
import sys
import uuid

import numpy as np

from qpython import CONVERSION_OPTIONS
from qpython import MetaData
from qpython.qcollection import QDictionary
from qpython.qcollection import QKeyedTable
from qpython.qcollection import qlist
from qpython.qcollection import qtable
from qpython.qcollection import QTable
from qpython.qtemporal import array_from_raw_qtemporal
from qpython.qtemporal import from_raw_qtemporal
from qpython.qtemporal import qtemporal
from qpython.qtype_fast import QException
from qpython.qtype_fast import QFunction
from qpython.qtype_fast import QLambda
from qpython.qtype_fast import QProjection

from qpython.qtype_fast cimport ATOM_SIZE
from qpython.qtype_fast cimport PY_TYPE
from qpython.qtype_fast cimport QADVERB_FUNC_106
from qpython.qtype_fast cimport QADVERB_FUNC_107
from qpython.qtype_fast cimport QADVERB_FUNC_108
from qpython.qtype_fast cimport QADVERB_FUNC_109
from qpython.qtype_fast cimport QADVERB_FUNC_110
from qpython.qtype_fast cimport QADVERB_FUNC_111
from qpython.qtype_fast cimport QBINARY_FUNC
from qpython.qtype_fast cimport QBOOL
from qpython.qtype_fast cimport QBOOL_LIST
from qpython.qtype_fast cimport QCHAR
from qpython.qtype_fast cimport QCOMPOSITION_FUNC
from qpython.qtype_fast cimport QDATE
from qpython.qtype_fast cimport QDATETIME
from qpython.qtype_fast cimport QDICTIONARY
from qpython.qtype_fast cimport QERROR
from qpython.qtype_fast cimport QGENERAL_LIST
from qpython.qtype_fast cimport QGUID
from qpython.qtype_fast cimport QGUID_LIST
from qpython.qtype_fast cimport QLAMBDA
from qpython.qtype_fast cimport QMINUTE
from qpython.qtype_fast cimport QMONTH
from qpython.qtype_fast cimport QNULL
from qpython.qtype_fast cimport QPROJECTION
from qpython.qtype_fast cimport QSECOND
from qpython.qtype_fast cimport QSTRING
from qpython.qtype_fast cimport QSYMBOL
from qpython.qtype_fast cimport QSYMBOL_LIST
from qpython.qtype_fast cimport QTABLE
from qpython.qtype_fast cimport QTERNARY_FUNC
from qpython.qtype_fast cimport QTIME
from qpython.qtype_fast cimport QTIME_LIST
from qpython.qtype_fast cimport QTIMESPAN
from qpython.qtype_fast cimport QTIMESTAMP
from qpython.qtype_fast cimport QTIMESTAMP_LIST
from qpython.qtype_fast cimport QUNARY_FUNC
from qpython.qtype_fast cimport STRUCT_MAP

from qpython.fastutils cimport uncompress


class QReaderException(Exception):
    """
    Indicates an error raised during data deserialization.
    """
    pass


cdef class QMessage:
    def __init__(self, data, message_type, message_size, compression_mode):
        self._data = data
        self._type = message_type
        self._size = message_size
        self._compression_mode = compression_mode

    @property
    def data(self):
        """Parsed data."""
        return self._data

    @data.setter
    def data(self, value):
        self._data = value

    @property
    def type(self):
        """Type of the message."""
        return self._type

    @property
    def compression_mode(self):
        """Indicates whether source message was compressed."""
        return self._compression_mode

    @property
    def size(self):
        """Size of the source message."""
        return self._size

    def __str__(self):
        return (
            f"QMessage: message type: {self._type}, data size: {self._size}, "
            f"compression_mode: {self._compression_mode}, data: {self._data}"
        )

cdef class BytesBuffer:
    def __init__(self):
        self._endianness = "@"

    @property
    def endianness(self) -> str:
        """Gets the endianness.

        :returns: Endianness of data.
        """
        return self._endianness

    @endianness.setter
    def endianness(self, str endianness):
        """
        Sets the byte order (endianness) for reading from the buffer.

        :Parameters:
            - `endianness` (``<`` or ``>``) - byte order indicator
        """
        self._endianness = endianness

    cpdef wrap(self, bytes data):
        """
        Wraps the data in the buffer.

        :Parameters:
            - `data` - data to be wrapped
        """
        self._data = data
        self._position = 0
        self._size = len(data)

    cpdef skip(self, int offset):
        """
        Skips reading of `offset` bytes.

        :Parameters:
            - `offset` (`integer`) - number of bytes to be skipped
        """
        new_position = self._position + offset

        if new_position > self._size:
            raise QReaderException("Attempt to read data out of buffer bounds")

        self._position = new_position

    cpdef bytes raw(self, int offset):
        """
        Gets `offset` number of raw bytes.

        :Parameters:
            - `offset` (`integer`) - number of bytes to be retrieved

        :returns: raw bytes
        """
        cdef int new_position
        cdef bytes raw
        new_position = self._position + offset

        if new_position > self._size:
            raise QReaderException("Attempt to read data out of buffer bounds")

        raw = self._data[self._position : new_position]
        self._position = new_position
        return raw

    cpdef get(self, str fmt):
        """
        Gets bytes from the buffer according to specified format or `offset`.

        :Parameters:
            - `fmt` (struct format) - conversion to be applied for reading

        :returns: unpacked bytes
        """
        cdef int offset
        fmt = self._endianness + fmt
        offset = struct.calcsize(fmt)
        return struct.unpack(fmt, self.raw(offset))[0]

    cpdef get_byte(self):
        """
        Gets a single byte from the buffer.

        :returns: single byte
        """
        return self.get("b")

    cpdef int get_int(self):
        """
        Gets a single 32-bit integer from the buffer.

        :returns: single integer
        """
        return self.get("i")

    cpdef get_uint(self):
        """
        Gets a single 32-bit unsigned integer from the buffer.

        :returns: single integer
        """
        return self.get("I")

    cpdef get_long(self):
        """
        Gets a single 64-bit integer from the buffer.

        :returns: single integer
        """
        return self.get("q")

    cpdef get_symbol(self):
        """
        Gets a single, ``\\x00`` terminated string from the buffer.

        :returns: ``\\x00`` terminated string
        """
        cdef int new_position
        cdef bytes raw
        new_position = self._data.find(b"\x00", self._position)

        if new_position < 0:
            raise QReaderException("Failed to read symbol from stream")

        raw = self._data[self._position : new_position]
        self._position = new_position + 1
        return raw

    cpdef get_symbols(self, int count):
        """
        Gets ``count`` ``\\x00`` terminated strings from the buffer.

        :Parameters:
            - `count` (`integer`) - number of strings to be read

        :returns: list of ``\\x00`` terminated string read from the buffer
        """
        cdef int new_position
        new_position = self._position

        if count == 0:
            return []

        results = self._data[new_position:].split(b'\x00', count)
        if len(results) != count + 1:
            raise QReaderException('Failed to read symbols from stream')

        self.wrap(results[-1])
        results = results[:-1]
        return results


cdef class QReader:
    def __init__(self, stream, str encoding="latin-1"):
        self._stream = stream
        self._buffer = BytesBuffer()
        self._encoding = encoding

        self._reader_map = {}
        self._reader_map[QERROR] = self._read_error
        self._reader_map[QSTRING] = self._read_string
        self._reader_map[QSYMBOL] = self._read_symbol
        self._reader_map[QCHAR] = self._read_char
        self._reader_map[QGUID] = self._read_guid
        self._reader_map[QTIMESPAN] = self._read_temporal
        self._reader_map[QTIMESTAMP] = self._read_temporal
        self._reader_map[QTIME] = self._read_temporal
        self._reader_map[QSECOND] = self._read_temporal
        self._reader_map[QMINUTE] = self._read_temporal
        self._reader_map[QDATE] = self._read_temporal
        self._reader_map[QMONTH] = self._read_temporal
        self._reader_map[QDATETIME] = self._read_temporal
        self._reader_map[QDICTIONARY] = self._read_dictionary
        self._reader_map[QTABLE] = self._read_table
        self._reader_map[QGENERAL_LIST] = self._read_general_list
        self._reader_map[QNULL] = self._read_function
        self._reader_map[QUNARY_FUNC] = self._read_function
        self._reader_map[QBINARY_FUNC] = self._read_function
        self._reader_map[QTERNARY_FUNC] = self._read_function
        self._reader_map[QLAMBDA] = self._read_lambda
        self._reader_map[QCOMPOSITION_FUNC] = self._read_function_composition
        self._reader_map[QADVERB_FUNC_106] = self._read_adverb_function
        self._reader_map[QADVERB_FUNC_107] = self._read_adverb_function
        self._reader_map[QADVERB_FUNC_108] = self._read_adverb_function
        self._reader_map[QADVERB_FUNC_109] = self._read_adverb_function
        self._reader_map[QADVERB_FUNC_110] = self._read_adverb_function
        self._reader_map[QADVERB_FUNC_111] = self._read_adverb_function
        self._reader_map[QPROJECTION] = self._read_projection

    def read(self, source = None, **options):
        """
        Reads and optionally parses a single message.

        :Parameters:
         - `source` - optional data buffer to be read, if not specified data is
           read from the wrapped stream
        :Options:
         - `raw` (`boolean`) - indicates whether read data should parsed or
           returned in raw byte form
         - `numpy_temporals` (`boolean`) - if ``False`` temporal vectors are
           backed by raw q representation (:class:`.QTemporalList`,
           :class:`.QTemporal`) instances, otherwise are represented as
           `numpy datetime64`/`timedelta64` arrays and atoms,
           **Default**: ``False``

        :returns: :class:`.QMessage` - read data (parsed or raw byte form) along
                  with meta information
        """
        message = self.read_header(source)
        message.data = self.read_data(message.size, message.compression_mode, **options)
        return message

    def read_header(self, source = None):
        """
        Reads and parses message header.

        .. note:: :func:`.read_header` wraps data for further reading in internal
                  buffer

        :Parameters:
         - `source` - optional data buffer to be read, if not specified data is
           read from the wrapped stream

        :returns: :class:`.QMessage` - read meta information
        """
        if self._stream:
            header = self._read_bytes(8)
            self._buffer.wrap(header)
        else:
            self._buffer.wrap(source)

        self._buffer.endianness = "<" if self._buffer.get_byte() == 1 else ">"
        self._is_native = self._buffer.endianness == ("<" if sys.byteorder == "little" else ">")
        message_type = self._buffer.get_byte()
        message_compression_mode = self._buffer.get_byte()
        message_size_ext = self._buffer.get_byte()

        message_size = self._buffer.get_uint()
        message_size += message_size_ext << 32
        return QMessage(None, message_type, message_size, message_compression_mode)

    def read_data(self, int message_size, int compression_mode = 0, **options):
        """
        Reads and optionally parses data part of a message.

        .. note:: :func:`.read_header` is required to be called before executing
                  the :func:`.read_data`

        :Parameters:
         - `message_size` (`int`) - size of the message to be read
         - `compression_mode` (`int`) - indicates whether data is compressed, 1 for <2GB, 2 for larger
        :Options:
         - `raw` (`boolean`) - indicates whether read data should parsed or
           returned in raw byte form
         - `numpy_temporals` (`boolean`) - if ``False`` temporal vectors are
           backed by raw q representation (:class:`.QTemporalList`,
           :class:`.QTemporal`) instances, otherwise are represented as
           `numpy datetime64`/`timedelta64` arrays and atoms,
           **Default**: ``False``

        :returns: read data (parsed or raw byte form)
        """
        self._options = MetaData(**CONVERSION_OPTIONS.union_dict(**options))
        cdef int comprHeaderLen
        cdef int uncompressed_size
        if compression_mode > 0:
            comprHeaderLen = 4 if compression_mode == 1 else 8
            if self._stream:
                self._buffer.wrap(self._read_bytes(comprHeaderLen))
            uncompressed_size = -8 + (self._buffer.get_uint() if compression_mode == 1 else self._buffer.get_long())
            compressed_data = self._read_bytes(message_size - (8+comprHeaderLen)) if self._stream else self._buffer.raw(message_size - (8+comprHeaderLen))

            raw_data = np.frombuffer(compressed_data, dtype = np.uint8)
            if  uncompressed_size <= 0:
                raise QReaderException("Error while data decompression.")

            raw_data = uncompress(raw_data, uncompressed_size)
            # raw_data = np.ndarray.tobytes(raw_data)
            raw_data = raw_data.tobytes()
            self._buffer.wrap(raw_data)
        elif self._stream:
            raw_data = self._read_bytes(message_size - 8)
            self._buffer.wrap(raw_data)
        if not self._stream and self._options.raw:
            raw_data = self._buffer.raw(message_size - 8)

        return raw_data if self._options.raw else self._read_object()

    cpdef _read_object(self):
        cdef int qtype
        qtype = self._buffer.get_byte()
        reader = self._reader_map.get(qtype, None)
        if reader:
            return reader(qtype)
        elif qtype >= QBOOL_LIST and qtype <= QTIME_LIST:
            return self._read_list(qtype)
        elif qtype <= QBOOL and qtype >= QTIME:
            return self._read_atom(qtype)
        raise QReaderException(f"Unable to deserialize q type: {hex(qtype)}")

    cpdef _read_error(self, int qtype):
        raise QException(self._read_symbol(QSYMBOL))

    cpdef _read_string(self, int qtype):
        cdef int length
        self._buffer.skip(1)  # ignore attributes
        length = self._buffer.get_int()
        return self._buffer.raw(length) if length > 0 else b''

    cpdef _read_symbol(self, int qtype):
        return np.string_(self._buffer.get_symbol())

    cpdef _read_char(self, int qtype):
        return chr(self._read_atom(QCHAR)).encode(self._encoding)

    cpdef _read_guid(self, int qtype):
        return uuid.UUID(bytes = self._buffer.raw(16))

    cpdef _read_atom(self, int qtype):
        cdef str fmt
        try:
            fmt = STRUCT_MAP[qtype]
            conversion = PY_TYPE[qtype]
            return conversion(self._buffer.get(fmt))
        except KeyError:
            raise QReaderException(f"Unable to deserialize q type: {hex(qtype)}")

    cpdef _read_temporal(self, int qtype):
        try:
            fmt = STRUCT_MAP[qtype]
            conversion = PY_TYPE[qtype]
            temporal = from_raw_qtemporal(conversion(self._buffer.get(fmt)), qtype=qtype)
            return temporal if self._options.numpy_temporals else qtemporal(temporal, qtype = qtype)
        except KeyError:
            raise QReaderException(f"Unable to deserialize q type: {hex(qtype)}")

    cpdef _read_list(self, int qtype):
        attr = self._buffer.get_byte()
        isLongLength = attr & 0x80 != 0
        length = self._buffer.get_long() if isLongLength else self._buffer.get_uint()
        conversion = PY_TYPE.get(-qtype, None)

        if qtype == QSYMBOL_LIST:
            symbols = self._buffer.get_symbols(length)
            data = np.array(symbols, dtype = np.string_)
            return qlist(data, qtype = qtype, adjust_dtype = False)
        elif qtype == QGUID_LIST:
            data = np.array([self._read_guid(QGUID) for x in range(length)])
            return qlist(data, qtype = qtype, adjust_dtype = False)
        elif conversion:
            raw = self._buffer.raw(length * ATOM_SIZE[qtype])
            data = np.frombuffer(raw, dtype = conversion)
            if not self._is_native:
                data.byteswap(True)

            if qtype >= QTIMESTAMP_LIST and qtype <= QTIME_LIST and self._options.numpy_temporals:
                data = array_from_raw_qtemporal(data, qtype)

            return qlist(data, qtype = qtype, adjust_dtype = False)
        else:
            raise QReaderException(f"Unable to deserialize q type: {hex(qtype)}")

    cpdef _read_dictionary(self, int qtype):
        keys = self._read_object()
        values = self._read_object()

        if isinstance(keys, QTable):
            return QKeyedTable(keys, values)
        else:
            return QDictionary(keys, values)

    cpdef _read_table(self, int qtype):
        self._buffer.skip(1)  # ignore attributes
        self._buffer.skip(1)  # ignore dict type stamp

        columns = self._read_object()
        data = self._read_object()

        return qtable(columns, data, qtype = QTABLE)

    cpdef _read_general_list(self, int qtype):
        cdef int i
        cdef int length
        self._buffer.skip(1)  # ignore attributes
        length = self._buffer.get_int()
        return [self._read_object() for i in range(length)]

    cpdef _read_function(self, int qtype):
        code = self._buffer.get_byte()
        return None if qtype == QNULL and code == 0 else QFunction(qtype)

    cpdef _read_lambda(self, int qtype):
        self._buffer.get_symbol()  # skip
        expression = self._read_object()
        return QLambda(expression.decode())

    cpdef _read_function_composition(self, int qtype):
        self._read_projection(qtype)  # skip
        return QFunction(qtype)

    cpdef _read_adverb_function(self, int qtype):
        self._read_object()  # skip
        return QFunction(qtype)

    cpdef _read_projection(self, int qtype):
        cdef int length
        length = self._buffer.get_int()
        parameters = [self._read_object() for x in range(length)]
        return QProjection(parameters)

    cpdef bytes _read_bytes(self, int length):
        cdef bytes data
        if not self._stream:
            raise QReaderException("There is no input data. QReader requires either stream or data chunk")

        if length == 0:
            return b""
        else:
            data = self._stream.read(length)

        if len(data) == 0:
            raise QReaderException("Error while reading data")
        return data
