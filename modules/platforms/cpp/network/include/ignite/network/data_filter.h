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

#ifndef _IGNITE_NETWORK_DATA_FILTER
#define _IGNITE_NETWORK_DATA_FILTER

#include <ignite/network/data_sink.h>
#include <ignite/network/async_handler.h>

namespace ignite
{
    namespace network
    {
        /**
         * Data buffer.
         */
        class IGNITE_IMPORT_EXPORT DataFilter : public DataSink, public AsyncHandler
        {
        public:
            /**
             * Default constructor.
             */
            DataFilter() :
                sink(0),
                handler(0)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            virtual ~DataFilter()
            {
                // No-op.
            }

            /**
             * Set sink.
             *
             * @param sink Data sink
             */
            void SetSink(DataSink* sink)
            {
                this->sink = sink;
            }

            /**
             * Get sink.
             *
             * @return Data sink.
             */
            DataSink* GetSink()
            {
                return sink;
            }

            /**
             * Set handler.
             *
             * @param handler Event handler.
             */
            void SetHandler(AsyncHandler* handler)
            {
                this->handler = handler;
            }

            /**
             * Get handler.
             *
             * @return Event handler.
             */
            AsyncHandler* GetHandler()
            {
                return handler;
            }

        protected:
            /** Sink. */
            DataSink* sink;

            /** Handler. */
            AsyncHandler* handler;
        };

        // Shared pointer type alias.
        typedef common::concurrent::SharedPointer<DataFilter> SP_DataFilter;
    }
}

#endif //_IGNITE_NETWORK_DATA_FILTER
