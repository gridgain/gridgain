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

project(ignite-network)

set(TARGET ${PROJECT_NAME})

find_package(OpenSSL)

include_directories(include src ${OPENSSL_INCLUDE_DIR})

set(SOURCES
        src/network/async_client_pool_adapter.cpp
        src/network/error_handling_filter.cpp
        src/network/codec_data_filter.cpp
        src/network/data_buffer.cpp
        src/network/length_prefix_codec.cpp
        src/network/network.cpp
        src/network/ssl/secure_data_filter.cpp
        src/network/ssl/secure_socket_client.cpp
        src/network/ssl/secure_utils.cpp
        src/network/ssl/ssl_gateway.cpp)

if (WIN32)
    include_directories(os/win/src)

    list(APPEND SOURCES
            os/win/src/network/tcp_socket_client.cpp
            os/win/src/network/sockets.cpp
            os/win/src/network/utils.cpp
            os/win/src/network/win_async_client.cpp
            os/win/src/network/win_async_client_pool.cpp
            os/win/src/network/win_async_connecting_thread.cpp
            os/win/src/network/win_async_worker_thread.cpp)
else()
    include_directories(os/linux/src)

    list(APPEND SOURCES
            os/linux/src/network/connecting_context.cpp
            os/linux/src/network/linux_async_client.cpp
            os/linux/src/network/linux_async_client_pool.cpp
            os/linux/src/network/linux_async_worker_thread.cpp
            os/linux/src/network/tcp_socket_client.cpp
            os/linux/src/network/sockets.cpp
            os/linux/src/network/utils.cpp)
endif()

list(APPEND _target_libs ${TARGET})
if (WIN32)
    add_library(${TARGET}-objlib OBJECT ${SOURCES})
    
    add_library(${TARGET} SHARED $<TARGET_OBJECTS:${TARGET}-objlib>)

    list(APPEND _target_libs ${TARGET}-objlib)
    
    set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME "ignite.network")
else()
    add_library(${TARGET} SHARED ${SOURCES})
endif()

foreach(_target_lib IN LISTS _target_libs)
    set_target_properties(${_target_lib} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

    if (${_target_lib} STREQUAL ${TARGET}-objlib)
        set_target_properties(${_target_lib} PROPERTIES POSITION_INDEPENDENT_CODE 1)

        target_link_libraries(${_target_lib} ignite-common-objlib ignite-binary-objlib)
    else()
        target_link_libraries(${_target_lib} ignite-common ignite-binary)
    endif()

    if (WIN32)
        target_link_libraries(${_target_lib} wsock32 ws2_32 iphlpapi crypt32)
    endif()

    target_include_directories(${_target_lib} INTERFACE include ${OS_INCLUDE})
endforeach()
unset(_target_libs)

install(TARGETS ${TARGET} 
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_PREFIX}/include FILES_MATCHING PATTERN "*.h*")
