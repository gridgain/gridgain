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

#ifndef _IGNITE_NETWORK_UTILS
#define _IGNITE_NETWORK_UTILS

#include <set>
#include <string>
#include <vector>
#include <algorithm>

#include <ignite/common/common.h>
#include <ignite/ignite_error.h>

namespace ignite
{
    namespace network
    {
        namespace utils
        {
            /**
             * Get set of local addresses.
             *
             * @param addrs Addresses set.
             */
            void IGNITE_IMPORT_EXPORT GetLocalAddresses(std::set<std::string>& addrs);

            /**
             * Throw connection error.
             *
             * @param err Error message.
             */
            inline void ThrowNetworkError(const std::string& err)
            {
                throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, err.c_str());
            }
        }
    }
}

#endif //_IGNITE_NETWORK_UTILS
