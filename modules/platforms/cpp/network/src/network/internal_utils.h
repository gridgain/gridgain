/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

#ifndef _IGNITE_NETWORK_INTERNAL_UTILS
#define _IGNITE_NETWORK_INTERNAL_UTILS

#include <set>
#include <string>
#include <vector>
#include <algorithm>

namespace ignite
{
    namespace network
    {
        namespace internal_utils
        {
            /**
             * Shuffle addresses randomly.
             *
             * @param addrsIn Addresses.
             * @return Randomly shuffled addresses.
             */
            template<typename Addrinfo>
            std::vector<Addrinfo*> ShuffleAddresses(Addrinfo* addrsIn)
            {
                std::vector<Addrinfo*> res;

                for (Addrinfo *it = addrsIn; it != NULL; it = it->ai_next)
                    res.push_back(it);

                std::random_shuffle(res.begin(), res.end());

                return res;
            }
        }
    }
}

#endif //_IGNITE_NETWORK_INTERNAL_UTILS
