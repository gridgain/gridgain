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
    /// <summary>
    /// Platform ids.
    /// <para />
    /// See <c>org.apache.ignite.internal.processors.platform.client.ClientPlatform</c> in Java.
    /// </summary>
    internal static class ClientPlatformId
    {
        /** Java platform code. */
        public const byte Java = 1;

        /** .NET platform code. */
        public const byte Dotnet = 2;
    }
}
