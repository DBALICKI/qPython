import numpy as np
import re
from functools import reduce


cdef int QNULL = 0x65
cdef int QGENERAL_LIST = 0x00
cdef int QBOOL = -0x01
cdef int QBOOL_LIST = 0x01
cdef int QGUID = -0x02
cdef int QGUID_LIST = 0x02
cdef int QBYTE = -0x04
cdef int QBYTE_LIST = 0x04
cdef int QSHORT = -0x05
cdef int QSHORT_LIST = 0x05
cdef int QINT = -0x06
cdef int QINT_LIST = 0x06
cdef int QLONG = -0x07
cdef int QLONG_LIST = 0x07
cdef int QFLOAT = -0x08
cdef int QFLOAT_LIST = 0x08
cdef int QDOUBLE = -0x09
cdef int QDOUBLE_LIST = 0x09
cdef int QCHAR = -0x0a
cdef int QSTRING = 0x0a
cdef int QSTRING_LIST = 0x00
cdef int QSYMBOL = -0x0b
cdef int QSYMBOL_LIST = 0x0b

cdef int QTIMESTAMP = -0x0c
cdef int QTIMESTAMP_LIST = 0x0c
cdef int QMONTH = -0x0d
cdef int QMONTH_LIST = 0x0d
cdef int QDATE = -0x0e
cdef int QDATE_LIST = 0x0e
cdef int QDATETIME = -0x0f
cdef int QDATETIME_LIST = 0x0f
cdef int QTIMESPAN = -0x10
cdef int QTIMESPAN_LIST = 0x10
cdef int QMINUTE = -0x11
cdef int QMINUTE_LIST = 0x11
cdef int QSECOND = -0x12
cdef int QSECOND_LIST = 0x12
cdef int QTIME = -0x13
cdef int QTIME_LIST = 0x13

cdef int QDICTIONARY = 0x63
cdef int QKEYED_TABLE = 0x63
cdef int QTABLE = 0x62
cdef int QLAMBDA = 0x64
cdef int QUNARY_FUNC = 0x65
cdef int QBINARY_FUNC = 0x66
cdef int QTERNARY_FUNC = 0x67
cdef int QCOMPOSITION_FUNC = 0x69
cdef int QADVERB_FUNC_106 = 0x6a
cdef int QADVERB_FUNC_107 = 0x6b
cdef int QADVERB_FUNC_108 = 0x6c
cdef int QADVERB_FUNC_109 = 0x6d
cdef int QADVERB_FUNC_110 = 0x6e
cdef int QADVERB_FUNC_111 = 0x6f
cdef int QPROJECTION = 0x68

cdef int QERROR = -0x80


cdef tuple ATOM_SIZE = (
    0,
    1,
    16,
    0,
    1,
    2,
    4,
    8,
    4,
    8,
    1,
    0,
    8,
    4,
    4,
    8,
    8,
    4,
    4,
    4,
)


# mapping of q types for Python binary translation
cdef dict STRUCT_MAP = {
    QBOOL: "b",
    QBYTE: "b",
    QSHORT: "h",
    QINT: "i",
    QLONG: "q",
    QFLOAT: "f",
    QDOUBLE: "d",
    QSTRING: "s",
    QSYMBOL: "S",
    QCHAR: "b",
    QMONTH: "i",
    QDATE: "i",
    QDATETIME: "d",
    QMINUTE: "i",
    QSECOND: "i",
    QTIME: "i",
    QTIMESPAN: "q",
    QTIMESTAMP: "q",
}


cdef dict PY_TYPE = {
    QBOOL: np.bool_,
    QBYTE: np.byte,
    QGUID: np.object_,
    QSHORT: np.int16,
    QINT: np.int32,
    QLONG: np.int64,
    QFLOAT: np.float32,
    QDOUBLE: np.float64,
    QCHAR: np.byte,
    QSYMBOL: np.string_,
    QMONTH: np.int32,
    QDATE: np.int32,
    QDATETIME: np.float64,
    QMINUTE: np.int32,
    QSECOND: np.int32,
    QTIME: np.int32,
    QTIMESTAMP: np.int64,
    QTIMESPAN: np.int64,
    # artificial qtype for convenient conversion of string lists
    QSTRING_LIST: np.object_,
}


class QException(Exception):
    """Represents a q error."""
    pass

class QFunction(object):
    """Represents a q function."""

    def __init__(self, qtype):
        self.qtype = qtype

    def __str__(self):
        return f"{self.__class__.__name__}#{self.qtype}"



class QLambda(QFunction):
    """Represents a q lambda expression.

    .. note:: `expression` is trimmed and required to be valid q function 
              (``{..}``) or k function (``k){..}``).

    :Parameters:
     - `expression` (`string`) - lambda expression

    :raises: `ValueError`
    """

    def __init__(self, expression):
        QFunction.__init__(self, QLAMBDA)

        if not expression:
            raise ValueError("Lambda expression cannot be None or empty")

        expression = expression.strip()

        if not QLambda._EXPRESSION_REGEX.match(expression):
            raise ValueError(f"Invalid lambda expression: {expression}")

        self.expression = expression

    _EXPRESSION_REGEX = re.compile(r'\s*(k\))?\s*\{.*\}')

    def __str__(self):
        return f"{self.__class__.__name__}(\'{self.expression}\')"

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and self.expression == other.expression)



class QProjection(QFunction):
    """Represents a q projection.

    :Parameters:
     - `parameters` (`list`) - list of parameters for lambda expression
    """

    def __init__(self, parameters):
        QFunction.__init__(self, QPROJECTION)
        self.parameters = parameters

    def __str__(self):
        parameters_str = []
        for arg in self.parameters:
            parameters_str.append(f"{arg}")
        return f"{self.__class__.__name__}({', '.join(parameters_str)})" 

    def __eq__(self, other):
        return (
            (not self.parameters and not other.parameters)
            or reduce(
                lambda v1, v2: v1 or v2, map(lambda v: v in self.parameters, other.parameters)
            )
        )

    def __ne__(self, other):
        return not self.__eq__(other)
