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
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Datastream;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client security permissions.
    /// </summary>
    public class ClientSecurityPermissionsTest
    {
        /** */
        private const string Login = "CLIENT";

        /** */
        private const string AllowAllLogin = "CLIENT_";

        /** */
        private const string ForbiddenCache = "FORBIDDEN_CACHE";

        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            TestUtils.EnsureJvmCreated();
            TestUtilsJni.StartIgnite("server");
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            TestUtilsJni.StopIgnite("server");
        }

        [Test]
        public void TestCreateCacheNoPermissionThrowsSecurityViolationClientException()
        {
            using (var client = StartClient())
            {
                var ex = Assert.Throws<IgniteClientException>(() => client.CreateCache<int, int>(ForbiddenCache));

                Assert.AreEqual(ClientStatusCode.SecurityViolation, ex.StatusCode);
            }
        }

        [Test]
        public void TestDataStreamerNoPermissionThrowsSecurityViolationClientException([Values(true, false)] bool add)
        {
            using (var client = StartClient(AllowAllLogin))
            {
                client.GetOrCreateCache<int, int>(ForbiddenCache);
            }

            using (var client = StartClient())
            {
                var options = new DataStreamerClientOptions {AllowOverwrite = true};
                var streamer = client.GetDataStreamer<int, int>(ForbiddenCache, options);

                if (add)
                {
                    streamer.Add(1, 1);
                }
                else
                {
                    streamer.Remove(1);
                }

                var ex = Assert.Throws<AggregateException>(() => streamer.Flush());
                var clientEx = (IgniteClientException)ex.GetBaseException();

                Assert.AreEqual(ClientStatusCode.SecurityViolation, clientEx.StatusCode);

                var perm = add ? "CACHE_PUT" : "CACHE_REMOVE";
                var message = Regex.Replace(clientEx.Message, "id=.*?, ", string.Empty);

                Assert.AreEqual(
                    "Authorization failed [perm=" + perm +
                    ", name=FORBIDDEN_CACHE, subject=TestSecuritySubject{type=REMOTE_CLIENT, login=CLIENT}]",
                    message);
            }
        }

        private static IIgniteClient StartClient(string login = Login)
        {
            return Ignition.StartClient(GetClientConfiguration(login));
        }

        private static IgniteClientConfiguration GetClientConfiguration(string login)
        {
            return new IgniteClientConfiguration("127.0.0.1")
            {
                UserName = login,
                Password = "pass1"
            };
        }
    }
}
