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
ACLOCAL_AMFLAGS =-I m4

lib_LTLIBRARIES = libignite-network.la

SUBDIRS = \
	include

AM_CPPFLAGS = \
	-I$(srcdir)/include \
	-I$(srcdir)/src \
	-I$(srcdir)/os/linux/include \
	-I$(srcdir)/os/linux/src \
	-I@top_srcdir@/common/include \
	-I@top_srcdir@/common/os/linux/include \
	-I@top_srcdir@/binary/include \
	-DIGNITE_IMPL \
	-D__STDC_LIMIT_MACROS \
	-D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
	-Wall \
	-std=c++03

libignite_network_la_LIBADD = \
	@top_srcdir@/common/libignite-common.la \
	@top_srcdir@/binary/libignite-binary.la

libignite_network_la_LDFLAGS = \
	-no-undefined \
	-version-info 0:0:0 \
	-release $(PACKAGE_VERSION)

libignite_network_la_DEPENDENCIES = \
	@top_srcdir@/common/libignite-common.la \
	@top_srcdir@/binary/libignite-binary.la

libignite_network_la_SOURCES = \
	os/linux/src/network/connecting_context.cpp \
	os/linux/src/network/linux_async_client.cpp \
	os/linux/src/network/linux_async_client_pool.cpp \
	os/linux/src/network/linux_async_worker_thread.cpp \
	os/linux/src/network/utils.cpp \
	os/linux/src/network/tcp_socket_client.cpp \
	os/linux/src/network/sockets.cpp \
	src/network/async_client_pool_adapter.cpp \
	src/network/codec_data_filter.cpp \
	src/network/data_buffer.cpp \
	src/network/error_handling_filter.cpp \
	src/network/length_prefix_codec.cpp \
	src/network/network.cpp \
	src/network/ssl/secure_data_filter.cpp \
	src/network/ssl/secure_socket_client.cpp \
    src/network/ssl/secure_utils.cpp \
	src/network/ssl/ssl_gateway.cpp

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
