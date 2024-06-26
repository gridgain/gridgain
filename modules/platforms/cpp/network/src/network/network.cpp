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

#include "network/sockets.h"

#ifdef _WIN32
#   include "network/win_async_client_pool.h"
#else // Other. Assume Linux
#   include "network/linux_async_client_pool.h"
#endif

#include <ignite/network/network.h>

#include "network/async_client_pool_adapter.h"
#include "network/ssl/ssl_gateway.h"
#include "network/ssl/secure_socket_client.h"
#include "network/tcp_socket_client.h"

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            IGNITE_IMPORT_EXPORT void EnsureSslLoaded()
            {
                SslGateway::GetInstance().LoadAll();
            }

            IGNITE_IMPORT_EXPORT SocketClient* MakeSecureSocketClient(const SecureConfiguration& cfg)
            {
                EnsureSslLoaded();

                return new SecureSocketClient(cfg);
            }
        }

        IGNITE_IMPORT_EXPORT SocketClient* MakeTcpSocketClient()
        {
            return new TcpSocketClient;
        }

        IGNITE_IMPORT_EXPORT SP_AsyncClientPool MakeAsyncClientPool(const std::vector<SP_DataFilter>& filters)
        {
            SP_AsyncClientPool platformPool = SP_AsyncClientPool(
#ifdef _WIN32
                new WinAsyncClientPool()
#else // Other. Assume Linux
                new LinuxAsyncClientPool()
#endif
            );

            return SP_AsyncClientPool(new AsyncClientPoolAdapter(filters, platformPool));
        }
    }
}
