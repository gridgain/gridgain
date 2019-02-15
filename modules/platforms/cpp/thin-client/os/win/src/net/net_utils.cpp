/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#include <cstddef>

#include <string>
#include <set>
#include <iostream>

#include <winsock2.h>
#include <ws2ipdef.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <iphlpapi.h>

#include <ignite/ignite_error.h>

#include "impl/net/net_utils.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace net
            {
                namespace net_utils
                {
                    void GetLocalAddresses(std::set<std::string>& addrs)
                    {
                        IP_ADAPTER_ADDRESSES outAddrs[64];

                        DWORD outAddrsSize = sizeof(outAddrs);

                        DWORD error = ::GetAdaptersAddresses(AF_UNSPEC, 0, NULL, &outAddrs[0], &outAddrsSize);

                        if (ERROR_SUCCESS != error)
                            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Error getting local addresses list");

                        for (IP_ADAPTER_ADDRESSES* outAddr = &outAddrs[0]; NULL != outAddr; outAddr = outAddr->Next)
                        {
                            if (IF_TYPE_SOFTWARE_LOOPBACK == outAddr->IfType)
                                continue;

                            for (IP_ADAPTER_UNICAST_ADDRESS* addr = outAddr->FirstUnicastAddress;
                                NULL != addr;
                                addr = addr->Next)
                            {
                                void *inAddr = 0;
                                
                                char strBuffer[INET6_ADDRSTRLEN] = { 0 };

                                int saFamily = addr->Address.lpSockaddr->sa_family;

                                switch (saFamily)
                                {
                                    case AF_INET:
                                    {
                                        SOCKADDR_IN* ipv4 = reinterpret_cast<SOCKADDR_IN*>(addr->Address.lpSockaddr);
                                        inAddr = &ipv4->sin_addr;

                                        break;
                                    }

                                    case AF_INET6:
                                    {
                                        SOCKADDR_IN6* ipv6 = reinterpret_cast<SOCKADDR_IN6*>(addr->Address.lpSockaddr);
                                        inAddr = &ipv6->sin6_addr;

                                        break;
                                    }

                                    default:
                                        continue;
                                }

                                inet_ntop(saFamily, inAddr, strBuffer, sizeof(strBuffer));

                                std::string strAddr(strBuffer);

                                if (!strAddr.empty())
                                    addrs.insert(strAddr);
                            }
                        }
                    }
                }
            }
        }
    }
}

