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

noinst_PROGRAMS = ignite-thin-client-tests

SUBDIRS = \
	include

AM_CPPFLAGS = \
	-I$(srcdir)/include \
	-I@top_srcdir@/core/include \
	-I@top_srcdir@/core/os/linux/include \
	-I@top_srcdir@/common/include \
	-I@top_srcdir@/common/os/linux/include \
	-I@top_srcdir@/binary/include \
	-I@top_srcdir@/network/include \
	-I@top_srcdir@/network/os/linux/include \
	-I@top_srcdir@/jni/include \
	-I@top_srcdir@/jni/os/linux/include \
	-I@top_srcdir@/thin-client/include \
	-I@top_srcdir@/thin-client/os/linux/include \
	-I$(JAVA_HOME)/include \
	-I$(JAVA_HOME)/include/linux \
	-DIGNITE_IMPL \
	-D__STDC_LIMIT_MACROS \
	-D__STDC_CONSTANT_MACROS \
	-DBOOST_TEST_DYN_LINK

AM_CXXFLAGS = \
	-Wall \
	-std=c++03

ignite_thin_client_tests_LDADD = \
	@top_srcdir@/core/libignite.la \
	@top_srcdir@/network/libignite-network.la \
	@top_srcdir@/thin-client/libignite-thin-client.la \
	-lpthread \
	-lboost_thread \
	-lboost_system \
	-lboost_chrono \
	-lboost_unit_test_framework

ignite_thin_client_tests_LDFLAGS = \
	-static-libtool-libs \
	-rdynamic

ignite_thin_client_tests_SOURCES = \
        src/teamcity/teamcity_boost.cpp \
        src/teamcity/teamcity_messages.cpp \
        src/cache_client_test.cpp \
        src/compute_client_test.cpp \
        src/continuous_query_test.cpp \
        src/test_utils.cpp \
        src/test_server.cpp \
        src/ignite_client_test.cpp \
        src/scan_query_test.cpp \
        src/sql_fields_query_test.cpp \
        src/auth_test.cpp \
        src/tx_test.cpp \
        src/ssl_test.cpp \
        src/interop_test.cpp

run-check: check
	./ignite-thin-client-tests -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-check:
	$(RM) $(ignite_thin_client_tests_OBJECTS)
