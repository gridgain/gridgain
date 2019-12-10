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

namespace Apache.Ignite.Core.Tests.Client
{
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client with old server versions.
    /// Differs from <see cref="ClientProtocolCompatibilityTest"/>:
    /// here we actually download and run old Ignite versions instead of changing the protocol version in handshake.
    /// </summary>
    public class ClientServerCompatibilityTest
    {
        [Test]
        public void TestCacheOperationsAreSupportedOnAllVersions([Values("8.4.6", "8.7.8")] string version)
        {
            using (JavaServer.Start(version))
            using (var client = StartClient())
            {
                var cache = client.GetOrCreateCache<int, int>(TestContext.CurrentContext.Test.Name);
                cache.Put(1, 10);
                Assert.AreEqual(10, cache.Get(1));
            }
        }

        [Test]
        public void TestClusterOperationsThrowCorrectExceptionOnVersionsOlderThan150(
            [Values("8.4.6", "8.7.8")] string version)
        {
            using (JavaServer.Start(version))
            using (var client = StartClient())
            {
                ClientProtocolCompatibilityTest.TestClusterOperationsThrowCorrectExceptionOnVersionsOlderThan150(
                    client, version);
            }
        }

        private static IIgniteClient StartClient()
        {
            return Ignition.StartClient(GetClientConfiguration());
        }

        private static IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration(JavaServer.GetClientConfiguration())
            {
                Logger = new ListLogger(new ConsoleLogger(LogLevel.Trace)),
                EnablePartitionAwareness = true
            };
        }
    }
}