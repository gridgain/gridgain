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

namespace Apache.Ignite.Core.Impl.Client
{
    using System;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Extension methods for <see cref="IClientSocket"/>.
    /// </summary>
    internal static class ClientSocketExtensions
    {
        /// <summary>
        /// Performs a send-receive operation.
        /// </summary>
        public static T DoOutInOp<T>(this IClientSocket socket, ClientOp opId, Action<IBinaryStream> writeAction,
            Func<IBinaryStream, T> readFunc, Func<ClientStatusCode, string, T> errorFunc = null)
        {
            return socket.DoOutInOp(opId, ctx => writeAction(ctx.Stream), readFunc, errorFunc);
        }

        /// <summary>
        /// Performs a send-receive operation asynchronously.
        /// </summary>
        public static Task<T> DoOutInOpAsync<T>(this IClientSocket socket,  ClientOp opId, 
            Action<IBinaryStream> writeAction,  Func<IBinaryStream, T> readFunc,
            Func<ClientStatusCode, string, T> errorFunc = null)
        {
            return socket.DoOutInOpAsync(opId, ctx => writeAction(ctx.Stream), readFunc, errorFunc);
        }
    }
}