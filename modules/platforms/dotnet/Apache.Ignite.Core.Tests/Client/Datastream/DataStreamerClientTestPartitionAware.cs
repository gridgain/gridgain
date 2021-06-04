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

namespace Apache.Ignite.Core.Tests.Client.Datastream
{
    using Apache.Ignite.Core.Client.Datastream;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IDataStreamerClient{TK,TV}"/>.
    /// </summary>
    [TestFixture]
    public class DataStreamerClientTestPartitionAware : DataStreamerClientTest
    {
        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientTest"/>.
        /// </summary>
        public DataStreamerClientTestPartitionAware()
            : base(enablePartitionAwareness: true)
        {
            // No-op.
        }
    }
}
