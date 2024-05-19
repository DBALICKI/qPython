import struct

from qpython.qtype_fast cimport QBOOL
from qpython.qtype_fast cimport QBOOL_LIST
from qpython.qtype_fast cimport QTIME
from qpython.qtype_fast cimport QTIME_LIST

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

    def wrap(self, bytes data):
        """
        Wraps the data in the buffer.

        :Parameters:
            - `data` - data to be wrapped
        """
        self._data = data
        self._position = 0
        self._size = len(data)

    def skip(self, int offset = 1):
        """
        Skips reading of `offset` bytes.

        :Parameters:
            - `offset` (`integer`) - number of bytes to be skipped
        """
        new_position = self._position + offset

        if new_position > self._size:
            raise QReaderException("Attempt to read data out of buffer bounds")

        self._position = new_position

    def raw(self, int offset) -> bytes:
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

    def get(self, str fmt, int offset = 0):
        """
        Gets bytes from the buffer according to specified format or `offset`.

        :Parameters:
            - `fmt` (struct format) - conversion to be applied for reading
            - `offset` (`integer`) - number of bytes to be retrieved

        :returns: unpacked bytes
        """
        fmt = self._endianness + fmt
        if not offset:
            offset = struct.calcsize(fmt)
        return struct.unpack(fmt, self.raw(offset))[0]

    def get_byte(self):
        """
        Gets a single byte from the buffer.

        :returns: single byte
        """
        return self.get("b")


    def get_int(self):
        """
        Gets a single 32-bit integer from the buffer.

        :returns: single integer
        """
        return self.get("i")


    def get_uint(self):
        """
        Gets a single 32-bit unsigned integer from the buffer.

        :returns: single integer
        """
        return self.get("I")


    def get_long(self):
        """
        Gets a single 64-bit integer from the buffer.

        :returns: single integer
        """
        return self.get("q")

    def get_symbol(self):
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

    def get_symbols(self, int count):
        """
        Gets ``count`` ``\\x00`` terminated strings from the buffer.

        :Parameters:
            - `count` (`integer`) - number of strings to be read

        :returns: list of ``\\x00`` terminated string read from the buffer
        """
        cdef int c
        cdef int new_position
        cdef bytes raw
        c = 0
        new_position = self._position

        if count == 0:
            return []

        while c < count:
            new_position = self._data.find(b"\x00", new_position)

            if new_position < 0:
                raise QReaderException("Failed to read symbol from stream")

            c += 1
            new_position += 1

        raw = self._data[self._position : new_position - 1]
        self._position = new_position

        return raw.split(b"\x00")


cdef class QReader:
    def __init__(self, stream, str encoding="latin-1"):
        self._stream = stream
        self._buffer = BytesBuffer()
        self._encoding = encoding

        self._reader_map = {}

    def _read_object(self):
        qtype = self._buffer.get_byte()
        reader = self._reader_map.get(qtype, None)
        if reader:
            return reader(self, qtype)
        elif qtype >= QBOOL_LIST and qtype <= QTIME_LIST:
            return self._read_list(qtype)
        elif qtype <= QBOOL and qtype >= QTIME:
            return self._read_atom(qtype)
        raise QReaderException('Unable to deserialize q type: %s' % hex(qtype))
