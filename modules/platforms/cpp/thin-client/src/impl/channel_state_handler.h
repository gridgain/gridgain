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

#ifndef _IGNITE_IMPL_THIN_CHANNEL_STATE_HANDLER
#define _IGNITE_IMPL_THIN_CHANNEL_STATE_HANDLER

#include <stdint.h>

#include <ignite/ignite_error.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /** Channel state handler. */
            class ChannelStateHandler
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~ChannelStateHandler()
                {
                    // No-op.
                }

                /**
                 * Channel handshake completion callback.
                 *
                 * @param id Channel ID.
                 */
                virtual void OnHandshakeSuccess(uint64_t id) = 0;

                /**
                 * Channel handshake error callback.
                 *
                 * @param id Channel ID.
                 * @param err Error.
                 */
                virtual void OnHandshakeError(uint64_t id, const IgniteError& err) = 0;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_CHANNEL_STATE_HANDLER