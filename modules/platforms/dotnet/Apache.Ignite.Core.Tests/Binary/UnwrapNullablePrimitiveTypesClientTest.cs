/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

namespace Apache.Ignite.Core.Tests.Binary
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="BinaryConfiguration.UnwrapNullablePrimitiveTypes"/> with client.
    /// </summary>
    [TestFixture]
    public class UnwrapNullablePrimitiveTypesClientTest : UnwrapNullablePrimitiveTypesTest
    {
        private IIgniteClient _client;

        [SetUp]
        public void ClientSetUp()
        {
            _client = Ignition.StartClient(new IgniteClientConfiguration("localhost:10800"));
        }

        [TearDown]
        public void ClientTearDown()
        {
            _client.Dispose();
        }
    }
}
