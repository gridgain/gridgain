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

project(ignite-thin-client)

set(TARGET ${PROJECT_NAME})

include_directories(include src)

set(SOURCES src/impl/data_channel.cpp
        src/impl/utility.cpp
        src/impl/protocol_version.cpp
        src/impl/ignite_node.cpp
        src/impl/ignite_client_impl.cpp
        src/impl/data_router.cpp
        src/impl/affinity/affinity_topology_version.cpp
        src/impl/affinity/affinity_assignment.cpp
        src/impl/affinity/affinity_manager.cpp
        src/impl/remote_type_updater.cpp
        src/impl/message.cpp
        src/impl/cache/cache_client_proxy.cpp
        src/impl/cache/cache_client_impl.cpp
        src/impl/transactions/transactions_impl.cpp
        src/impl/transactions/transactions_proxy.cpp
        src/ignite_client.cpp)

add_library(${TARGET} SHARED ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

if (WIN32)
	set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME "ignite.thin-client")
endif()

target_link_libraries(${TARGET} ignite-binary ignite-network)

target_include_directories(${TARGET} INTERFACE include)

install(TARGETS ${TARGET} LIBRARY DESTINATION lib)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_PREFIX}/include FILES_MATCHING PATTERN "*.h*")
