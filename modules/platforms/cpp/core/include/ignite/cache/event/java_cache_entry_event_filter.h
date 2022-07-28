/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file
 * Declares ignite::cache::event::JavaCacheEntryEventFilter class.
 */

#ifndef _IGNITE_CACHE_EVENT_JAVA_CACHE_ENTRY_EVENT_FILTER
#define _IGNITE_CACHE_EVENT_JAVA_CACHE_ENTRY_EVENT_FILTER

#include <ignite/cache/event/cache_entry_event.h>
#include <ignite/impl/cache/event/cache_entry_event_filter_base.h>

namespace ignite
{
    class IgniteBinding;

    namespace cache
    {
        namespace event
        {
            /**
             * Java cache entry event filter.
             *
             * All templated types should be default-constructable,
             * copy-constructable and assignable.
             *
             * @tparam K Key type.
             * @tparam V Value type.
             */
            template<typename K, typename V>
            class JavaCacheEntryEventFilter
            {
            public:
                /**
                 * Default constructor.
                 */
                JavaCacheEntryEventFilter()
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~JavaCacheEntryEventFilter()
                {
                    // No-op.
                }
            };
        }
    }
}

#endif //_IGNITE_CACHE_EVENT_JAVA_CACHE_ENTRY_EVENT_FILTER