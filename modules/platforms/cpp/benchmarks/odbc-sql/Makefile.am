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

noinst_PROGRAMS = ignite-odbc-sql-benchmark

AM_CPPFLAGS = \
    -I@top_srcdir@/include \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++11

ignite_odbc_sql_benchmark_LDADD = \
    -lodbc \
    -lpthread \
    -lboost_thread \
    -lboost_system \
    -lboost_chrono \
    -lboost_program_options

ignite_odbc_sql_benchmark_LDFLAGS = \
    -static-libtool-libs

ignite_odbc_sql_benchmark_SOURCES = \
    src/odbc_sql.cpp

run-check: check
	./ignite-odbc-sql-benchmark -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-check:
	$(RM) $(ignite_odbc_sql_benchmark_OBJECTS)
