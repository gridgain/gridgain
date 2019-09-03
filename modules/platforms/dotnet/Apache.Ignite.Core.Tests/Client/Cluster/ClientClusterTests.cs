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

namespace Apache.Ignite.Core.Tests.Client.Cluster
{
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    ///  Cluster API tests for thin client.
    /// </summary>
    public class ClientClusterTests : ClientTestBase
    {
        /** Cache name. */
        private const string PersistentCache = "persistentCache";

        /** Persistence data region name. */
        private const string DataRegionName = "persistenceRegion";

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public override void TestSetUp()
        {
            var cacheCfg = new CacheConfiguration()
            {
                Name = PersistentCache,
                DataRegionName = DataRegionName,
            };

            var ignite = Ignition.GetIgnite();
            //to make sure there is no persisted cache from previous runs
            ignite.DestroyCache(PersistentCache);
            ignite.GetOrCreateCache<int, int>(cacheCfg);
        }

        /** <inheritDoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            var baseConfig = base.GetIgniteConfiguration();
            baseConfig.DataStorageConfiguration.DataRegionConfigurations = new []
            {
                new DataRegionConfiguration
                {
                    Name = DataRegionName,
                    PersistenceEnabled = true
                }
            };

            return baseConfig;
        }

        /// <summary>
        /// Test cluster activation
        /// </summary>
        [Test]
        public void TestClusterActivation()
        {
            var clientCluster = Client.GetCluster();
            clientCluster.SetActive(true);
            Assert.IsTrue(clientCluster.IsActive());
        }

        /// <summary>
        /// Test cluster activation
        /// </summary>
        [Test]
        public void TestClusterDeactivation()
        {
            var clientCluster = Client.GetCluster();
            try
            {
                clientCluster.SetActive(false);
                Assert.IsFalse(clientCluster.IsActive());
            }
            finally
            {
                //tear down logic requires active cluster
                clientCluster.SetActive(true);
            }
        }

        [Test]
        public void TestForAttributesRequest()
        {
            var clientCluster = Client.GetCluster();
            var dotNetCluster = clientCluster.ForDotNet();

            Assert.IsNotNull(dotNetCluster);
        }

        [Test]
        public void TestEnableWal()
        {

            var clientCluster = Client.GetCluster();
            clientCluster.SetActive(true);
            clientCluster.EnableWal(PersistentCache);
            Assert.IsTrue(clientCluster.IsWalEnabled(PersistentCache));
        }

        [Test]
        public void TestDisableWal()
        {
            var clientCluster = Client.GetCluster();
            clientCluster.SetActive(true);
            clientCluster.DisableWal(PersistentCache);
            Assert.IsFalse(clientCluster.IsWalEnabled(PersistentCache));
        }
    }
}
