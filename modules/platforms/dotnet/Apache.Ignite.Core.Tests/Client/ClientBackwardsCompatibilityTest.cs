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
    using System.Linq;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Log;
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
            // Use a non-existent version that is not supported by the server
            var version = new ClientProtocolVersion(short.MaxValue, short.MaxValue, short.MaxValue);
            
            using (var client = GetClient(version))
            {
                Assert.AreEqual(ClientSocket.CurrentProtocolVersion, client.ServerVersion);

                var logs = GetLogs(client);
                
                var expectedMessage = "Handshake failed on 127.0.0.1:10800, " +
                                      "requested protocol version = 32767.32767.32767, server protocol version = , " +
                                      "status = Fail, message = Unsupported version.";

                var message = Regex.Replace(
                    logs[2].Message, @"server protocol version = \d\.\d\.\d", "server protocol version = ");
                
                Assert.AreEqual(expectedMessage, message);
            }
        }

        /// <summary>
        /// Tests that old client with new server can negotiate a protocol version.
        /// </summary>
        [Test]
        public void TestClientOlderThanServerConnectsOnClientVersion([Values(0, 1, 2, 3, 4, 5)] short minor)
        {
            var version = new ClientProtocolVersion(1, minor, 0);

            using (var client = GetClient(version))
            {
                Assert.AreEqual(version, client.ServerVersion);

                var lastLog = GetLogs(client).Last();
                var expectedLog = string.Format(
                    "Handshake completed on 127.0.0.1:10800, protocol version = {0}", version);
                
                Assert.AreEqual(expectedLog, lastLog.Message);
                Assert.AreEqual(LogLevel.Debug, lastLog.Level);
                Assert.AreEqual(nameof(ClientSocket), lastLog.Category);
            }
        }

        /// <summary>
        /// Gets the client with specified protocol version.
        /// </summary>
        private IgniteClient GetClient(ClientProtocolVersion version)
        {
            var cfg = new IgniteClientConfiguration(GetClientConfiguration())
            {
                ProtocolVersion = version
            };

            return (IgniteClient) Ignition.StartClient(cfg);
        }
    }
}