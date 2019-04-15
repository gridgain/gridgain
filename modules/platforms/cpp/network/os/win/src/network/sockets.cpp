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

#include "network/sockets.h"

#include <sstream>

#include <ignite/network/socket_client.h>

namespace ignite
{
    namespace network
    {
        namespace sockets
        {
            int GetLastSocketError()
            {
                return WSAGetLastError();
            }

            int GetLastSocketError(int handle)
            {
                int lastError = 0;
                socklen_t size = sizeof(lastError);
                int res = getsockopt(handle, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&lastError), &size);

                return res == SOCKET_ERROR ? 0 : lastError;
            }

            std::string GetSocketErrorMessage(HRESULT error)
            {
                std::stringstream res;

                res << "error_code=" << error;

                if (error == 0)
                    return res.str();

                LPTSTR errorText = NULL;

                DWORD len = FormatMessage(
                    // use system message tables to retrieve error text
                    FORMAT_MESSAGE_FROM_SYSTEM
                    // allocate buffer on local heap for error text
                    | FORMAT_MESSAGE_ALLOCATE_BUFFER
                    // We're not passing insertion parameters
                    | FORMAT_MESSAGE_IGNORE_INSERTS,
                    // unused with FORMAT_MESSAGE_FROM_SYSTEM
                    NULL,
                    error,
                    MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US),
                    // output
                    reinterpret_cast<LPTSTR>(&errorText),
                    // minimum size for output buffer
                    0,
                    // arguments - see note
                    NULL);

                if (NULL != errorText && len > 0)
                {
                    std::string msg(reinterpret_cast<const char*>(errorText), static_cast<size_t>(len));

                    if (len != 0)
                        res << ", msg=" << msg;

                    LocalFree(errorText);
                }

                return res.str();
            }

            std::string GetLastSocketErrorMessage()
            {
                HRESULT lastError = WSAGetLastError();

                return GetSocketErrorMessage(lastError);
            }

            int WaitOnSocket(SocketHandle socket, int32_t timeout, bool rd)
            {
                int ready = 0;
                int lastError = 0;

                fd_set fds;

                do {
                    struct timeval tv = { 0 };
                    tv.tv_sec = timeout;

                    FD_ZERO(&fds);
                    FD_SET(socket, &fds);

                    fd_set* readFds = 0;
                    fd_set* writeFds = 0;

                    if (rd)
                        readFds = &fds;
                    else
                        writeFds = &fds;

                    ready = select(static_cast<int>(socket) + 1, readFds, writeFds, NULL, timeout == 0 ? NULL : &tv);

                    if (ready == SOCKET_ERROR)
                        lastError = GetLastSocketError();

                } while (ready == SOCKET_ERROR && IsSocketOperationInterrupted(lastError));

                if (ready == SOCKET_ERROR)
                    return -lastError;

                if (ready == 0)
                    return SocketClient::WaitResult::TIMEOUT;

                return SocketClient::WaitResult::SUCCESS;
            }

            bool IsSocketOperationInterrupted(int errorCode)
            {
                return errorCode == WSAEINTR;
            }
        }
    }
}
