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

#ifndef _IGNITE_NETWORK_SSL_SECURE_CONFIGURATION
#define _IGNITE_NETWORK_SSL_SECURE_CONFIGURATION

#include <string>

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            /**
             * TLS/SSL configuration parameters.
             */
            struct SecureConfiguration
            {
                /** Path to file containing security certificate to use. */
                std::string certPath;

                /** Path to file containing private key to use. */
                std::string keyPath;

                /** Path to file containing Certificate authority to use. */
                std::string caPath;
            };
        }
    }
}

#endif //_IGNITE_NETWORK_SSL_SECURE_CONFIGURATION
