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
    using System.Collections.Generic;

    /// <summary>
    /// Extension methods for <see cref="ClientOp"/>.
    /// </summary>
    internal static class ClientOpExtensions
    {
        /** */
        private static readonly Dictionary<ClientOp, ClientProtocolVersion> VersionMap = GetVersionMap();

        /// <summary>
        /// Gets minimum protocol version that is required to perform specified operation.
        /// </summary>
        /// <param name="op">Operation.</param>
        /// <returns>Minimum protocol version.</returns>
        public static ClientProtocolVersion GetMinVersion(this ClientOp op)
        {
            ClientProtocolVersion minVersion;
            
            return VersionMap.TryGetValue(op, out minVersion) 
                ? minVersion 
                : ClientSocket.Ver100;
        }
        
        /// <summary>
        /// Gets the version map.
        /// </summary>
        private static Dictionary<ClientOp, ClientProtocolVersion> GetVersionMap()
        {
            throw new System.NotImplementedException();
        }
    }
}