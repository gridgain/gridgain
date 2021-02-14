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
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Thin client binary configuration.
    /// </summary>
    internal class BinaryConfigurationClientInternal
    {
        /** */
        private readonly bool _compactFooter;

        /** */
        private readonly BinaryNameMapperMode _nameMapperMode;

        /// <summary>
        /// Initializes a new instance of <see cref="BinaryConfigurationClientInternal"/> class.
        /// </summary>
        public BinaryConfigurationClientInternal(IBinaryStream stream)
        {
            Debug.Assert(stream != null);

            _compactFooter = stream.ReadBool();
            _nameMapperMode = (BinaryNameMapperMode) stream.ReadByte();
        }

        /// <summary>
        /// Gets a flag indicating whether compact footer mode should be used.
        /// </summary>
        public bool CompactFooter
        {
            get { return _compactFooter; }
        }

        /// <summary>
        /// Gets the binary name mapper mode.
        /// </summary>
        public BinaryNameMapperMode NameMapperMode
        {
            get { return _nameMapperMode; }
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            return string.Format("BinaryConfigurationClientInternal [CompactFooter={0}, NameMapperMode={1}]",
                CompactFooter, NameMapperMode);
        }
    }
}
