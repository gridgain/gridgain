/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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
 * Declares ignite::thin::cache::event::CacheEntryEventListener class.
 */

#ifndef _IGNITE_THIN_CACHE_EVENT_CACHE_ENTRY_EVENT_LISTENER
#define _IGNITE_THIN_CACHE_EVENT_CACHE_ENTRY_EVENT_LISTENER

#include <stdint.h>

#include <ignite/thin/cache/event/cache_entry_event.h>

namespace ignite
{
    namespace thin
    {
        namespace cache
        {
            namespace event
            {
                /**
                 * Cache entry event listener.
                 */
                template<typename K, typename V>
                class CacheEntryEventListener
                {
                public:
                    /**
                     * Default constructor.
                     */
                    CacheEntryEventListener()
                    {
                        // No-op.
                    }

                    /**
                     * Destructor.
                     */
                    virtual ~CacheEntryEventListener()
                    {
                        // No-op.
                    }

                    /**
                     * Event callback.
                     *
                     * @param evts Events.
                     * @param num Events number.
                     */
                    virtual void OnEvent(const CacheEntryEvent<K, V>* evts, uint32_t num) = 0;

                    /**
                     * Disconnected callback.
                     *
                     * Called if channel was disconnected. This also means that continuous query was closed and no more
                     * events will be provided for this listener.
                     */
                    virtual void OnDisconnected() = 0;
                };
            }
        }
    }
}

#endif //_IGNITE_THIN_CACHE_EVENT_CACHE_ENTRY_EVENT_LISTENER