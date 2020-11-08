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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System.IO;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests that <see cref="QueryEntity.KeyTypeName"/> and <see cref="QueryEntity.ValueTypeName"/>
    /// settings trigger binary metadata registration on cache start for the specified types.
    /// <para />
    /// Normally, binary metadata is registered in the cluster when an object of the given type is first serialized
    /// (for cache storage or other purposes - Services, Compute, etc).
    /// However, query engine requires metadata for key/value types on cache start, so an eager registration
    /// should be performed.
    /// </summary>
    [Ignore("IGNITE-13607")]
    public class ClientQueryEntityMetadataRegistrationTestJavaOnlyServer : ClientQueryEntityMetadataRegistrationTest
    {
        /** */
        private const string StartTask = "org.apache.ignite.platform.PlatformStartIgniteTask";

        /** */
        private const string StopTask = "org.apache.ignite.platform.PlatformStopIgniteTask";

        /** */
        private static readonly IgniteConfiguration TempConfig =TestUtils.GetTestConfiguration(name: "tmp");

        /** */
        private string _javaNodeName;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public override void FixtureSetUp()
        {
            var springConfig = Path.Combine("Config", "query-entity-metadata-registration.xml");

            using (var ignite = Ignition.Start(TempConfig))
            {
                _javaNodeName = ignite.GetCompute().ExecuteJavaTask<string>(StartTask, springConfig);
                Assert.IsTrue(ignite.WaitTopology(2, 5000));
            }
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public override void FixtureTearDown()
        {
            using (var ignite = Ignition.Start(TempConfig))
            {
                ignite.GetCompute().ExecuteJavaTask<object>(StopTask, _javaNodeName);
                Assert.IsTrue(ignite.WaitTopology(1, 5000));
            }
        }
    }
}
