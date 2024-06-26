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

#ifndef _IGNITE_NETWORK_CODEC
#define _IGNITE_NETWORK_CODEC

#include <ignite/ignite_error.h>

#include <ignite/common/factory.h>
#include <ignite/impl/interop/interop_memory.h>

#include <ignite/network/data_buffer.h>

namespace ignite
{
    namespace network
    {
        /**
         * Codec class.
         * Encodes and decodes data.
         */
        class IGNITE_IMPORT_EXPORT Codec
        {
        public:
            /**
             * Destructor.
             */
            virtual ~Codec()
            {
                // No-op.
            }

            /**
             * Encode provided data.
             *
             * @param data Data to encode.
             * @return Encoded data. Returning null is ok.
             *
             * @throw IgniteError on error.
             */
            virtual DataBuffer Encode(DataBuffer& data) = 0;

            /**
             * Decode provided data.
             *
             * @param data Data to decode.
             * @return Decoded data. Returning null means data is not yet ready.
             *
             * @throw IgniteError on error.
             */
            virtual DataBuffer Decode(DataBuffer& data) = 0;
        };

        // Shared pointer codec type alias.
        typedef common::concurrent::SharedPointer<Codec> SP_Codec;

        /** Codec factory. */
        typedef common::Factory<Codec> CodecFactory;

        // Shared pointer to codec factory type alias.
        typedef common::concurrent::SharedPointer<CodecFactory> SP_CodecFactory;
    }
}

#endif //_IGNITE_NETWORK_CODEC