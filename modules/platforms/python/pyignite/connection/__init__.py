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

"""
This module contains `Connection` class, that wraps TCP socket handling,
as well as Ignite protocol handshaking.
"""

from collections import OrderedDict
import socket
from typing import Union

from pyignite.constants import *
from pyignite.exceptions import HandshakeError, ParameterError, SocketError
from pyignite.datatypes import Byte, Int, Short, String, UUIDObject
from pyignite.datatypes.internal import Struct
from pyignite.utils import select_version

from .handshake import HandshakeRequest
from .ssl import wrap


__all__ = ['Connection']


class Connection:
    """
    This is a `pyignite` class, that represents a connection to Ignite
    node. It serves multiple purposes:

     * socket wrapper. Detects fragmentation and network errors. See also
       https://docs.python.org/3/howto/sockets.html,
     * binary protocol connector. Incapsulates handshake, data read-ahead and
       failover reconnection.
    """

    _socket = None
    client = None
    host = None
    port = None
    timeout = None
    prefetch = None
    username = None
    password = None
    ssl_params = {}

    @staticmethod
    def _check_ssl_params(params):
        expected_args = [
            'use_ssl',
            'ssl_version',
            'ssl_ciphers',
            'ssl_cert_reqs',
            'ssl_keyfile',
            'ssl_certfile',
            'ssl_ca_certfile',
        ]
        for param in params:
            if param not in expected_args:
                raise ParameterError((
                    'Unexpected parameter for connection initialization: `{}`'
                ).format(param))

    def __init__(
        self, client: 'Client', prefetch: bytes = b'', timeout: int = None,
        username: str = None, password: str = None, **ssl_params
    ):
        """
        Initialize connection.

        For the use of the SSL-related parameters see
        https://docs.python.org/3/library/ssl.html#ssl-certificates.

        :param client: Ignite client object,
        :param prefetch: (optional) initialize the read-ahead data buffer.
         Empty by default,
        :param timeout: (optional) sets timeout (in seconds) for each socket
         operation including `connect`. 0 means non-blocking mode, which is
         virtually guaranteed to fail. Can accept integer or float value.
         Default is None (blocking mode),
        :param use_ssl: (optional) set to True if Ignite server uses SSL
         on its binary connector. Defaults to use SSL when username
         and password has been supplied, not to use SSL otherwise,
        :param ssl_version: (optional) SSL version constant from standard
         `ssl` module. Defaults to TLS v1.1, as in Ignite 2.5,
        :param ssl_ciphers: (optional) ciphers to use. If not provided,
         `ssl` default ciphers are used,
        :param ssl_cert_reqs: (optional) determines how the remote side
         certificate is treated:

         * `ssl.CERT_NONE` − remote certificate is ignored (default),
         * `ssl.CERT_OPTIONAL` − remote certificate will be validated,
           if provided,
         * `ssl.CERT_REQUIRED` − valid remote certificate is required,

        :param ssl_keyfile: (optional) a path to SSL key file to identify
         local (client) party,
        :param ssl_certfile: (optional) a path to ssl certificate file
         to identify local (client) party,
        :param ssl_ca_certfile: (optional) a path to a trusted certificate
         or a certificate chain. Required to check the validity of the remote
         (server-side) certificate,
        :param username: (optional) user name to authenticate to Ignite
         cluster,
        :param password: (optional) password to authenticate to Ignite cluster.
        """
        self.client = client
        self.prefetch = prefetch
        self.timeout = timeout
        self.username = username
        self.password = password
        self._check_ssl_params(ssl_params)
        if all([self.username, self.password, 'use_ssl' not in ssl_params]):
            ssl_params['use_ssl'] = True
        self.ssl_params = ssl_params

    _wrap = wrap

    def get_protocol_version(self):
        return self.client.protocol_version

    @select_version
    def read_response(self) -> Union[dict, OrderedDict]:
        """
        Processes server's response to the handshake request.

        :return: handshake data.
        """
        response_start = Struct([
            ('length', Int),
            ('op_code', Byte),
        ])
        start_class, start_buffer = response_start.parse(self)
        start = start_class.from_buffer_copy(start_buffer)
        data = response_start.to_python(start)
        if data['op_code'] == 0:
            response_end = Struct([
                ('version_major', Short),
                ('version_minor', Short),
                ('version_patch', Short),
                ('message', String),
            ])
        else:
            response_end = Struct([
                ('node_uuid', UUIDObject),
            ])
        end_class, end_buffer = response_end.parse(self)
        end = end_class.from_buffer_copy(end_buffer)
        data.update(response_end.to_python(end))
        return data

    def read_response_120(self):
        """
        Processes server's response to the handshake request (thin protocol
        version 1.2.0).

        :return: handshake data.
        """
        response_start = Struct([
            ('length', Int),
            ('op_code', Byte),
        ])
        start_class, start_buffer = response_start.parse(self)
        start = start_class.from_buffer_copy(start_buffer)
        data = response_start.to_python(start)
        if data['op_code'] == 0:
            response_end = Struct([
                ('version_major', Short),
                ('version_minor', Short),
                ('version_patch', Short),
                ('message', String),
            ])
            end_class, end_buffer = response_end.parse(self)
            end = end_class.from_buffer_copy(end_buffer)
            data.update(response_end.to_python(end))
        return data

    @property
    def socket(self) -> socket.socket:
        """
        Network socket.
        """
        if self._socket is None:
            self.connect(self.host, self.port)
        return self._socket

    def __repr__(self) -> str:
        if self.host and self.port:
            return '{}:{}'.format(self.host, self.port)
        else:
            return '<not connected>'

    def connect(
        self, host: str = None, port: int = None
    ) -> Union[dict, OrderedDict]:
        """
        Connect to the given server node with protocol version fallback.

        :param host: Ignite server node's host name or IP,
        :param port: Ignite server node's port number.
        """

        # choose highest version first
        if self.client.protocol_version is None:
            self.client.protocol_version = max(PROTOCOLS)

        try:
            return self._connect_version(host, port)
        except HandshakeError as e:
            if e.expected_version in PROTOCOLS:
                self.client.protocol_version = e.expected_version
                return self._connect_version(host, port)
            raise e

    def _connect_version(
        self, host: str = None, port: int = None,
    ) -> Union[dict, OrderedDict]:
        """
        Connect to the given server node using protocol version
        defined on client.

        :param host: Ignite server node's host name or IP,
        :param port: Ignite server node's port number.
        """
        host = host or IGNITE_DEFAULT_HOST
        port = port or IGNITE_DEFAULT_PORT

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.timeout)
        self._socket = self._wrap(self.socket)
        self._socket.connect((host, port))

        protocol_version = self.client.protocol_version

        hs_request = HandshakeRequest(
            protocol_version,
            self.username,
            self.password
        )
        self.send(hs_request)
        hs_response = self.read_response()
        if hs_response['op_code'] == 0:
            self.close()
            error_text = 'Handshake error: {}'.format(hs_response['message'])
            # if handshake fails for any reason other than protocol mismatch
            # (i.e. authentication error), server version is 0.0.0
            if any([
                hs_response['version_major'],
                hs_response['version_minor'],
                hs_response['version_patch'],
            ]):
                error_text += (
                    ' Server expects binary protocol version '
                    '{version_major}.{version_minor}.{version_patch}. Client '
                    'provides {client_major}.{client_minor}.{client_patch}.'
                ).format(
                    client_major=protocol_version[0],
                    client_minor=protocol_version[1],
                    client_patch=protocol_version[2],
                    **hs_response
                )
            raise HandshakeError((
                hs_response['version_major'],
                hs_response['version_minor'],
                hs_response['version_patch'],
            ), error_text)
        self.host, self.port = host, port
        return hs_response

    def reconnect(self):
        """
        Tries to reconnect
        """
        if self.host and self.port:
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
            self.connect(self.host, self.port)

    def _transfer_params(self, to: 'Connection'):
        """
        Transfer non-SSL parameters to target connection object.

        :param to: connection object to transfer parameters to.
        """
        to.username = self.username
        to.password = self.password
        to.client = self.client

    def clone(self, prefetch: bytes = b'') -> 'Connection':
        """
        Clones this connection in its current state.

        :return: `Connection` object.
        """
        clone = self.__class__(self.client, **self.ssl_params)
        self._transfer_params(to=clone)
        if self.port and self.host:
            clone.connect(self.host, self.port)
        clone.prefetch = prefetch
        return clone

    def send(self, data: bytes, flags=None):
        """
        Send data down the socket.

        :param data: bytes to send,
        :param flags: (optional) OS-specific flags.
        """
        kwargs = {}
        if flags is not None:
            kwargs['flags'] = flags
        data = bytes(data)
        total_bytes_sent = 0

        while total_bytes_sent < len(data):
            try:
                bytes_sent = self.socket.send(
                    data[total_bytes_sent:],
                    **kwargs
                )
            except OSError:
                self._socket = self.host = self.port = None
                raise
            if bytes_sent == 0:
                self.socket.close()
                raise SocketError('Socket connection broken.')
            total_bytes_sent += bytes_sent

    def recv(self, buffersize, flags=None) -> bytes:
        """
        Receive data from socket or read-ahead buffer.

        :param buffersize: bytes to receive,
        :param flags: (optional) OS-specific flags,
        :return: data received.
        """
        pref_size = len(self.prefetch)
        if buffersize > pref_size:
            result = self.prefetch
            self.prefetch = b''
            try:
                result += self._recv(buffersize-pref_size, flags)
            except (SocketError, OSError):
                self._socket = self.host = self.port = None
                raise
            return result
        else:
            result = self.prefetch[:buffersize]
            self.prefetch = self.prefetch[buffersize:]
            return result

    def _recv(self, buffersize, flags=None) -> bytes:
        """
        Handle socket data reading.
        """
        kwargs = {}
        if flags is not None:
            kwargs['flags'] = flags
        chunks = []
        bytes_rcvd = 0

        while bytes_rcvd < buffersize:
            chunk = self.socket.recv(buffersize-bytes_rcvd, **kwargs)
            if chunk == b'':
                self.socket.close()
                raise SocketError('Socket connection broken.')
            chunks.append(chunk)
            bytes_rcvd += len(chunk)

        return b''.join(chunks)

    def close(self):
        """
        Mark socket closed. This is recommended but not required, since
        sockets are automatically closed when they are garbage-collected.
        """
        if self._socket:
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
