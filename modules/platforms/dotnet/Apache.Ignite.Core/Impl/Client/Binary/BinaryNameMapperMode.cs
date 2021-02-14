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

namespace Apache.Ignite.Core.Impl.Client.Binary
{
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Represents the binary name mapper mode.
    /// </summary>
    internal enum BinaryNameMapperMode
    {
        /// <summary>
        /// Default full name mapper, see <see cref="BinaryBasicNameMapper.FullNameInstance"/>.
        /// </summary>
        BasicFull = 0,

        /// <summary>
        /// Simple name mapper, see <see cref="BinaryBasicNameMapper.SimpleNameInstance"/>.
        /// </summary>
        BasicSimple = 1,

        /// <summary>
        /// Custom user-defined mapper.
        /// </summary>
        Custom = 2
    }
}
