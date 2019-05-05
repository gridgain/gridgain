# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ctypes
from functools import wraps
from typing import Any, Type, Tuple, Union

from pyignite.datatypes.base import IgniteDataType
from .constants import *


def is_pow2(value: int) -> bool:
    """ Check if value is power of two. """
    return value > 0 and ((value & (value - 1)) == 0)


def is_iterable(value: Any) -> bool:
    """ Check if value is iterable. """
    try:
        iter(value)
        return True
    except TypeError:
        return False


def is_binary(value):
    """
    Check if a value is a pythonic representation of a Complex object.
    """
    return all([
        hasattr(value, 'type_name'),
        hasattr(value, 'type_id'),
        hasattr(value, 'schema'),
        hasattr(value, 'schema_id'),
    ])


def is_hinted(value):
    """
    Check if a value is a tuple of data item and its type hint.
    """
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and issubclass(value[1], IgniteDataType)
    )


def is_wrapped(value: Any) -> bool:
    """
    Check if a value is of WrappedDataObject type.
    """
    return (
        type(value) is tuple
        and len(value) == 2
        and type(value[0]) is bytes
        and type(value[1]) is int
    )


def int_overflow(value: int) -> int:
    """
    Simulates 32bit integer overflow.
    """
    return ((value ^ 0x80000000) & 0xffffffff) - 0x80000000


def unwrap_binary(client: 'Client', wrapped: tuple) -> object:
    """
    Unwrap wrapped BinaryObject and convert it to Python data.

    :param client: connection to Ignite cluster,
    :param wrapped: `WrappedDataObject` value,
    :return: dict representing wrapped BinaryObject.
    """
    from pyignite.datatypes.complex import BinaryObject

    blob, offset = wrapped
    conn_clone = client.random_node.clone(prefetch=blob)
    conn_clone.pos = offset
    data_class, data_bytes = BinaryObject.parse(conn_clone)
    result = BinaryObject.to_python(
        data_class.from_buffer_copy(data_bytes),
        client,
    )
    conn_clone.close()
    return result


def hashcode(string: Union[str, bytes]) -> int:
    """
    Calculate hash code used for identifying objects in Ignite binary API.

    :param string: UTF-8-encoded string identifier of binary buffer,
    :return: hash code.
    """
    result = 0
    for char in string:
        try:
            char = ord(char)
        except TypeError:
            pass
        result = int_overflow(31 * result + char)
    return result


def cache_id(cache: Union[str, int]) -> int:
    """
    Create a cache ID from cache name.

    :param cache: cache name or ID,
    :return: cache ID.
    """
    return cache if type(cache) is int else hashcode(cache)


def entity_id(cache: Union[str, int]) -> int:
    """
    Create a type ID from type name or field ID from field name.

    :param cache: entity name or ID,
    :return: entity ID.
    """
    return cache if type(cache) is int else hashcode(cache.lower())


def schema_id(schema: Union[int, dict]) -> int:
    """
    Calculate Complex Object schema ID.

    :param schema: a dict of field names: field types,
    :return: schema ID.
    """
    if type(schema) is int:
        return schema
    if schema is None:
        return 0
    s_id = FNV1_OFFSET_BASIS if schema else 0
    for field_name in schema.keys():
        field_id = entity_id(field_name)
        s_id ^= (field_id & 0xff)
        s_id = int_overflow(s_id * FNV1_PRIME)
        s_id ^= ((field_id >> 8) & 0xff)
        s_id = int_overflow(s_id * FNV1_PRIME)
        s_id ^= ((field_id >> 16) & 0xff)
        s_id = int_overflow(s_id * FNV1_PRIME)
        s_id ^= ((field_id >> 24) & 0xff)
        s_id = int_overflow(s_id * FNV1_PRIME)
    return s_id


def status_to_exception(exc: Type[Exception]):
    """
    Converts erroneous status code with error message to an exception
    of the given class.

    :param exc: the class of exception to raise,
    :return: decorator.
    """
    def ste_decorator(fn):
        @wraps(fn)
        def ste_wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)
            if result.status != 0:
                raise exc(result.message)
            return result.value
        return ste_wrapper
    return ste_decorator


def get_field_by_id(
    obj: 'GenericObjectMeta', field_id: int
) -> Tuple[Any, IgniteDataType]:
    """
    Returns a complex object's field value, given the field's entity ID.

    :param obj: complex object,
    :param field_id: field ID,
    :return: complex object field's value and type.
    """
    for fname, ftype in obj._schema.items():
        if entity_id(fname) == field_id:
            return getattr(obj, fname, getattr(ftype, 'default')), ftype


def unsigned(value: int, c_type: ctypes._SimpleCData = ctypes.c_uint) -> int:
    return c_type(value).value
