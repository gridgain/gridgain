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

nobase_include_HEADERS = \
	ignite/network/async_client_pool.h \
	ignite/network/async_handler.h \
	ignite/network/codec.h \
	ignite/network/codec_data_filter.h \
	ignite/network/data_buffer.h \
	ignite/network/data_filter.h \
	ignite/network/data_filter_adapter.h \
	ignite/network/data_sink.h \
	ignite/network/end_point.h \
	ignite/network/length_prefix_codec.h \
	ignite/network/network.h \
	ignite/network/socket_client.h \
	ignite/network/ssl/secure_configuration.h \
	ignite/network/ssl/secure_data_filter.h \
	ignite/network/tcp_range.h \
	ignite/network/utils.h

uninstall-hook:
	if [ -d ${includedir}/ignite ]; then find ${includedir}/ignite -type d -empty -delete; fi
