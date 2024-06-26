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
#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([Apache Ignite C++], [8.9.127.6496], [dev@ignite.apache.org], [ignite], [ignite.apache.org])

AC_CANONICAL_HOST
AC_CONFIG_MACRO_DIR([m4])
AC_LANG([C++])

AM_INIT_AUTOMAKE([-Wall foreign subdir-objects])
AC_CONFIG_HEADERS(config.h)

# Enable silent rules by default
AM_SILENT_RULES([yes])

# Checks for programs.
AC_PROG_CPP
AM_PROG_AR

# Initialize Libtool
LT_INIT

AC_ARG_ENABLE([odbc],
    [AS_HELP_STRING([--enable-odbc],[build included ODBC driver [default=yes]])],
    [ case "${enableval}" in
        yes) odbc=true ;;
        no) odbc=false ;;
        *) AC_MSG_ERROR(bad value ${enableval} for --enable-odbc) ;;
        esac
    ],
    [odbc=true]
)

AC_ARG_ENABLE([core],
    [AS_HELP_STRING([--enable-core],[build Ignite core library [default=yes]])],
    [ case "${enableval}" in
        yes) core=true ;;
        no) core=false ;;
        *) AC_MSG_ERROR(bad value ${enableval} for --enable-core) ;;
        esac],
    [core=true]
)

AC_ARG_ENABLE([thin-client],
    [AS_HELP_STRING([--enable-thin-client],[build Thin C++ client [default=yes]])],
    [ case "${enableval}" in
        yes) thin_client=true ;;
        no) thin_client=false ;;
        *) AC_MSG_ERROR(bad value ${enableval} for --enable-thin-client) ;;
        esac
    ],
    [thin_client=true]
)

AC_ARG_ENABLE([node],
    [AS_HELP_STRING([--enable-node],[build stand-alone node binary [default=yes]])],
    [ case "${enableval}" in
        yes) node=true ;;
        no) node=false ;;
        *) AC_MSG_ERROR(bad value ${enableval} for --enable-node) ;;
        esac],
    [node=true]
)

AC_ARG_ENABLE([tests],
    [AS_HELP_STRING([--enable-tests],[build tests for enabled components [default=yes]])],
    [ case "${enableval}" in
        yes) tests=true ;;
        no) tests=false ;;
        *) AC_MSG_ERROR(bad value ${enableval} for --enable-tests) ;;
        esac],
    [tests=true]
)

AM_CONDITIONAL([COND_ODBC], [test "x$odbc" = "xtrue"])
AM_CONDITIONAL([COND_CORE], [test "x$core" = "xtrue"])
AM_CONDITIONAL([COND_THIN_CLIENT], [test "x$thin_client" = "xtrue"])
AM_CONDITIONAL([COND_NODE], [test "x$node" = "xtrue"])
AM_CONDITIONAL([COND_TESTS], [test "x$tests" = "xtrue"])

AC_CONFIG_FILES([ \
    Makefile \
    core-test/include/Makefile \
    core-test/Makefile \
    common/os/linux/include/Makefile \
    common/include/Makefile \
    common/Makefile \
    binary/include/Makefile \
    binary/Makefile \
    network/include/Makefile \
    network/Makefile \
    odbc/include/Makefile \
    odbc/Makefile \
    odbc-test/include/Makefile \
    odbc-test/Makefile \
    core/include/Makefile \
    core/Makefile \
    core/ignite.pc \
    thin-client/include/Makefile \
    thin-client/Makefile \
    thin-client-test/include/Makefile \
    thin-client-test/Makefile \
    ignite/Makefile \
])

AC_OUTPUT
