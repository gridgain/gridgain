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

find_path(IGNITE_INCLUDE_DIR ignite/ignite.h
        HINTS ${IGNITE_CPP_DIR}/include
        PATH_SUFFIXES ignite)

if (WIN32)
	find_library(IGNITE_LIB ignite.core HINTS ${IGNITE_CPP_DIR}/lib)

	find_library(IGNITE_COMMON_LIB ignite.common HINTS ${IGNITE_CPP_DIR}/lib)

	find_library(IGNITE_NETWORK_LIB ignite.network HINTS ${IGNITE_CPP_DIR}/lib)

	find_library(IGNITE_JNI_LIB ignite.jni HINTS ${IGNITE_CPP_DIR}/lib)

	find_library(IGNITE_THIN_CLIENT_LIB ignite.thin-client HINTS ${IGNITE_CPP_DIR}/lib)

	find_library(IGNITE_BINARY_LIB ignite.binary HINTS ${IGNITE_CPP_DIR}/lib)
else()
	find_library(IGNITE_LIB ignite HINTS ${IGNITE_CPP_DIR}/lib)

	find_library(IGNITE_COMMON_LIB ignite-common HINTS ${IGNITE_CPP_DIR}/lib)

	find_library(IGNITE_NETWORK_LIB ignite-network HINTS ${IGNITE_CPP_DIR}/lib)

	find_library(IGNITE_JNI_LIB ignite-jni HINTS ${IGNITE_CPP_DIR}/lib)

	find_library(IGNITE_THIN_CLIENT_LIB ignite-thin-client HINTS ${IGNITE_CPP_DIR}/lib)

	find_library(IGNITE_BINARY_LIB ignite-binary HINTS ${IGNITE_CPP_DIR}/lib)	
endif()

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(Ignite DEFAULT_MSG
        IGNITE_LIB
        IGNITE_THIN_CLIENT_LIB
        IGNITE_BINARY_LIB
        IGNITE_NETWORK_LIB
        IGNITE_COMMON_LIB
        IGNITE_INCLUDE_DIR)
