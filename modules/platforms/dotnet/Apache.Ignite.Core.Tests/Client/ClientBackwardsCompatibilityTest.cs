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
    using Apache.Ignite.Core.Impl.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client with old protocol versions.
    /// </summary>
    public class ClientBackwardsCompatibilityTest : ClientTestBase
    {
        /// <summary>
        /// Tests that basic cache operations are supported on all protocols.
        /// </summary>
        [Test]
        public void TestCacheOperationsAreSupportedOnAllProtocols()
        {
            // TODO
        }

        [Test]
        public void TestClusterOperationsThrowCorrectExceptionOnVersionsOlderThan150()
        {
            
        }

        [Test]
        public void TestAffinityAwarenessDisablesAutomaticallyOnVersionsOlderThan140()
        {
            
        }

        [Test]
        public void TestClientNewerThanServerReconnectsOnServerVersion()
        {
            var cfg = new IgniteClientConfiguration(GetClientConfiguration())
            {
                // Use a non-existent version that is not supported by the server
                ProtocolVersion = new ClientProtocolVersion(short.MaxValue, short.MaxValue, short.MaxValue) 
            };

            using (var client = Ignition.StartClient(cfg))
            {
                var clientInternal = (IgniteClient) client;
                
                Assert.AreEqual(ClientSocket.CurrentProtocolVersion, clientInternal.ServerVersion);
            }
        }
    }
}