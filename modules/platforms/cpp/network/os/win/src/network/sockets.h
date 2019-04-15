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

#ifndef _IGNITE_NETWORK_SOCKETS
#define _IGNITE_NETWORK_SOCKETS

#define WIN32_LEAN_AND_MEAN
#define _WINSOCKAPI_

#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mstcpip.h>

#include <stdint.h>
#include <string>

namespace ignite
{
    namespace network
    {
        namespace sockets
        {
            /** Socket handle type. */
            typedef SOCKET SocketHandle;

            /**
             * Get socket error.
             * @return Last socket error.
             */
            int GetLastSocketError();

            /**
             * Get socket error.
             * @param handle Socket handle.
             * @return Last socket error.
             */
            int GetLastSocketError(int handle);

            /**
             * Get socket error message for the error code.
             * @param error Error code.
             * @return Socket error message string.
             */
            std::string GetSocketErrorMessage(HRESULT error);

            /**
             * Get last socket error message.
             * @return Last socket error message string.
             */
            std::string GetLastSocketErrorMessage();

            /**
             * Check whether socket operation was interupted.
             * @return @c true if the socket operation was interupted.
             */
            bool IsSocketOperationInterrupted(int errorCode);

            /**
             * Wait on the socket for any event for specified time.
             * This function uses poll to achive timeout functionality
             * for every separate socket operation.
             *
             * @param socket Socket handle.
             * @param timeout Timeout.
             * @param rd Wait for read if @c true, or for write if @c false.
             * @return -errno on error, WaitResult::TIMEOUT on timeout and
             *     WaitResult::SUCCESS on success.
             */
            int WaitOnSocket(SocketHandle socket, int32_t timeout, bool rd);
        }
    }
}

#endif //_IGNITE_NETWORK_SOCKETS