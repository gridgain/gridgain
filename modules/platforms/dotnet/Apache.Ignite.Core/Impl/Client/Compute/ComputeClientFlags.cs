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

namespace Apache.Ignite.Core.Impl.Client.Compute
{
    using System;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Client compute flags.
    /// </summary>
    [Flags]
    [SuppressMessage("Design", "CA1028:Enum Storage should be Int32", 
        Justification = "Dictated by thin client protocol.")]
    internal enum ComputeClientFlags : byte
    {
        None = 0,
        NoFailover = 1,
        NoResultCache = 2,
        KeepBinary = 4
    }
}