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
    ignite/thin/ssl_mode.h \
    ignite/thin/ignite_client.h \
    ignite/thin/ignite_client_configuration.h \
    ignite/thin/cache/cache_client.h \
    ignite/thin/cache/cache_entry.h \
    ignite/thin/cache/cache_peek_mode.h \
    ignite/thin/cache/event/cache_entry_event.h \
    ignite/thin/cache/event/cache_entry_event_listener.h \
    ignite/thin/cache/event/java_cache_entry_event_filter.h \
    ignite/thin/cache/query/continuous/continuous_query_client.h \
    ignite/thin/cache/query/continuous/continuous_query_handle.h \
    ignite/thin/cache/query/query_fields_cursor.h \
    ignite/thin/cache/query/query_fields_row.h \
    ignite/thin/cache/query/query_cursor.h \
    ignite/thin/cache/query/query_scan.h \
    ignite/thin/cache/query/query_sql_fields.h \
    ignite/thin/compute/compute_client.h \
    ignite/impl/thin/copyable.h \
    ignite/impl/thin/copyable_writable.h \
    ignite/impl/thin/platform_java_object_factory_proxy.h \
    ignite/impl/thin/writable_key.h \
    ignite/impl/thin/readable.h \
    ignite/impl/thin/writable.h \
    ignite/impl/thin/cache/query/query_cursor_proxy.h \
    ignite/impl/thin/cache/cache_client_proxy.h \
    ignite/impl/thin/cache/continuous/continuous_query_client_holder.h

uninstall-hook:
	if [ -d ${includedir}/ignite ]; then find ${includedir}/ignite -type d -empty -delete; fi
