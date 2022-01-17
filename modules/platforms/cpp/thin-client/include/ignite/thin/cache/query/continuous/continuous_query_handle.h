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
 * Declares ignite::thin::cache::query::continuous::ContinuousQueryHandleClient class.
 */

#ifndef _IGNITE_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_CLIENT
#define _IGNITE_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_CLIENT

#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace thin
    {
        namespace cache
        {
            namespace query
            {
                namespace continuous
                {
                    /**
                     * Continuous query handle client.
                     *
                     * Once destructed, continuous query is stopped and event listener stops getting event.
                     */
                    class ContinuousQueryHandleClient
                    {
                    public:
                        /**
                         * Default constructor.
                         */
                        ContinuousQueryHandleClient() :
                            impl()
                        {
                            // No-op.
                        }

                        /**
                         * Constructor.
                         *
                         * Internal method. Should not be used by user.
                         *
                         * @param impl Implementation.
                         */
                        ContinuousQueryHandleClient(const common::concurrent::SharedPointer<void>& impl) :
                            impl(impl)
                        {
                            // No-op.
                        }

                    private:
                        /** Implementation delegate. */
                        common::concurrent::SharedPointer<void> impl;
                    };
                }
            }
        }
    }
}

#endif //_IGNITE_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_CLIENT