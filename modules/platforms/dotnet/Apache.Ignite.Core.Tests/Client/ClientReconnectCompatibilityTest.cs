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
    using System;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client compatibility with reconnect.
    /// </summary>
    public class ClientReconnectCompatibilityTest
    {
        /// <summary>
        /// Tests that client reconnect to an old server node disables affinity awareness.
        /// </summary>
        [Test]
        public void TestReconnectToOldNodeDisablesPartitionAwareness()
        {
            IIgniteClient client = null;
            var clientConfiguration = new IgniteClientConfiguration(JavaServer.GetClientConfiguration())
            {
                EnablePartitionAwareness = true
            };
            
            try
            {
                using (StartNewServer())
                {
                    client = Ignition.StartClient(clientConfiguration);
                    var cache = client.GetOrCreateCache<int, int>(TestContext.CurrentContext.Test.Name);
                    cache.Put(1, 42);
                    Assert.AreEqual(42, cache.Get(1));
                    Assert.IsTrue(client.GetConfiguration().EnablePartitionAwareness);
                }

                Assert.Throws<IgniteClientException>(() => client.GetCacheNames());

                using (StartOldServer())
                {
                    var cache = client.GetOrCreateCache<int, int>(TestContext.CurrentContext.Test.Name);
                    cache.Put(1, 42);
                    Assert.AreEqual(42, cache.Get(1));
                    Assert.IsFalse(client.GetConfiguration().EnablePartitionAwareness);
                }
            }
            finally
            {
                if (client != null)
                {
                    client.Dispose();
                }
            }
        }

        /// <summary>
        /// Starts new server node (partition awareness is supported).
        /// </summary>
        private static IDisposable StartNewServer()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    Port = JavaServer.ClientPort
                }
            };

            return Ignition.Start(cfg);
        }

        /// <summary>
        /// Starts old server node (partition awareness is not supported).
        /// </summary>
        private static IDisposable StartOldServer()
        {
            return JavaServer.Start("2.4.0");
        }
    }
}