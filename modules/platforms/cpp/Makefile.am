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

if COND_THIN_CLIENT
  MAYBE_THIN_CLIENT = thin-client
  MAYBE_NETWORK = network

if COND_TESTS
    MAYBE_THIN_CLIENT_TESTS = thin-client-test
endif
endif

if COND_ODBC
  MAYBE_ODBC = odbc
  MAYBE_NETWORK = network

if COND_TESTS
    MAYBE_ODBC_TESTS = odbc-test
endif
endif

if COND_CORE
  MAYBE_CORE = core

if COND_TESTS
    MAYBE_CORE_TESTS = core-test
endif
endif

if COND_NODE
  MAYBE_CORE = core
  MAYBE_NODE = ignite
endif

SUBDIRS = \
    common \
    binary \
    $(MAYBE_NETWORK) \
    $(MAYBE_ODBC) \
    $(MAYBE_CORE) \
    $(MAYBE_THIN_CLIENT) \
    $(MAYBE_NODE) \
    $(MAYBE_ODBC_TESTS) \
    $(MAYBE_CORE_TESTS) \
    $(MAYBE_THIN_CLIENT_TESTS)

DIST_SUBDIRS = \
    common \
    binary \
    network \
    odbc \
    core \
    thin-client \
    ignite \
    core-test \
    odbc-test \
    thin-client-test
