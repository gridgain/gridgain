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

cmake_minimum_required(VERSION 3.6)
project(examples)

set(CMAKE_CXX_STANDARD 98)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/../cmake")

if (WIN32)
    add_definitions(-DUNICODE=1)

    add_compile_options(/source-charset:utf-8)

    set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
endif()

add_subdirectory(compute-example)
add_subdirectory(continuous-query-example)
add_subdirectory(odbc-example)
add_subdirectory(put-get-example)
add_subdirectory(query-example)
add_subdirectory(thin-client-put-get-example)
add_subdirectory(cluster-compute-example)
