#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import ctypes

from pyignite.constants import *
from .base import IgniteDataType


__all__ = [
    'Primitive',
    'Byte', 'Short', 'Int', 'Long', 'Float', 'Double', 'Char', 'Bool',
]


class Primitive(IgniteDataType):
    """
    Ignite primitive type. Base type for the following types:

    - Byte,
    - Short,
    - Int,
    - Long,
    - Float,
    - Double,
    - Char,
    - Bool.
    """

    c_type = None

    @classmethod
    def parse(cls, client: 'Client'):
        return cls.c_type, client.recv(ctypes.sizeof(cls.c_type))

    @staticmethod
    def to_python(ctype_object, *args, **kwargs):
        return ctype_object

    @classmethod
    def from_python(cls, value):
        return bytes(cls.c_type(value))


class Byte(Primitive):
    c_type = ctypes.c_byte
    _type_name = 'java.lang.Byte'


class Short(Primitive):
    c_type = ctypes.c_short
    _type_name = 'java.lang.Short'


class Int(Primitive):
    c_type = ctypes.c_int
    _type_name = 'java.lang.Integer'


class Long(Primitive):
    c_type = ctypes.c_longlong
    _type_name = 'java.lang.Long'


class Float(Primitive):
    c_type = ctypes.c_float
    _type_name = 'java.lang.Float'


class Double(Primitive):
    c_type = ctypes.c_double
    _type_name = 'java.lang.Double'


class Char(Primitive):
    c_type = ctypes.c_short

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        return ctype_object.value.to_bytes(
            ctypes.sizeof(cls.c_type),
            byteorder=PROTOCOL_BYTE_ORDER
        ).decode(PROTOCOL_CHAR_ENCODING)

    @classmethod
    def from_python(cls, value):
        if type(value) is str:
            value = value.encode(PROTOCOL_CHAR_ENCODING)
        # assuming either a bytes or an integer
        if type(value) is bytes:
            value = int.from_bytes(value, byteorder=PROTOCOL_BYTE_ORDER)
        # assuming a valid integer
        return value.to_bytes(
            ctypes.sizeof(cls.c_type),
            byteorder=PROTOCOL_BYTE_ORDER
        )


class Bool(Primitive):
    c_type = ctypes.c_bool
    _type_name = 'java.lang.Boolean'
