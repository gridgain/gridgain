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
    using System;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
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
            var cacheCfg = new CacheConfiguration
            {
                Name = PersistentCache,
                DataRegionName = DataRegionName
            };

            var ignite = Ignition.GetIgnite();
            GetCluster().SetActive(true);

            // To make sure there is no persisted cache from previous runs.
            ignite.DestroyCache(PersistentCache);
            ignite.GetOrCreateCache<int, int>(cacheCfg);
        }

        /** <inheritDoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            var baseConfig = base.GetIgniteConfiguration();

            baseConfig.DataStorageConfiguration = new DataStorageConfiguration
            {
                DataRegionConfigurations = new[]
                {
                    new DataRegionConfiguration
                    {
                        Name = DataRegionName,
                        PersistenceEnabled = true
                    }
                }
            };

            return baseConfig;
        }

        /// <summary>
        /// Test cluster activation.
        /// </summary>
        [Test]
        public void TestClusterActivation()
        {
            var clientCluster = GetCluster();
            clientCluster.SetActive(true);

            Assert.IsTrue(clientCluster.IsActive());
        }

        /// <summary>
        /// Test cluster deactivation.
        /// </summary>
        [Test]
        public void TestClusterDeactivation()
        {
            var clientCluster = GetCluster();
            clientCluster.SetActive(false);

            Assert.IsFalse(clientCluster.IsActive());
        }

        /// <summary>
        /// Test enable WAL.
        /// </summary>
        [Test]
        public void TestEnableWal()
        {
            var clientCluster = GetCluster();
            clientCluster.DisableWal(PersistentCache);

            Assert.IsTrue(clientCluster.EnableWal(PersistentCache));
            Assert.IsTrue(clientCluster.IsWalEnabled(PersistentCache));
        }

        /// <summary>
        /// Test enable WAL multiple times.
        /// </summary>
        [Test]
        public void TestEnableWalReturnsFalseIfWalWasEnabledBefore()
        {
            var clientCluster = GetCluster();
            clientCluster.DisableWal(PersistentCache);

            Assert.IsTrue(clientCluster.EnableWal(PersistentCache));
            Assert.IsFalse(clientCluster.EnableWal(PersistentCache));
        }

        /// <summary>
        /// Test enable WAL validates cache name.
        /// </summary>
        [TestCase(null)]
        [TestCase("")]
        public void TestEnableWalValidatesCacheNameArgument(string cacheName)
        {
            Assert.Throws<ArgumentException>(() => GetCluster().EnableWal(cacheName),
                "'cacheName' argument should not be null or empty.");
        }

        /// <summary>
        /// Test WAL enabled method validates cache name.
        /// </summary>
        [TestCase(null)]
        [TestCase("")]
        public void TestIsWalEnabledValidatesCacheNameArgument(string cacheName)
        {
            Assert.Throws<ArgumentException>(() => GetCluster().IsWalEnabled(cacheName),
                "'cacheName' argument should not be null or empty.");
        }

        /// <summary>
        /// Test disable WAL.
        /// </summary>
        [Test]
        public void TestDisableWal()
        {
            var clientCluster = GetCluster();
            clientCluster.EnableWal(PersistentCache);

            Assert.IsTrue(clientCluster.DisableWal(PersistentCache));
            Assert.IsFalse(clientCluster.IsWalEnabled(PersistentCache));
        }

        /// <summary>
        /// Test disable WAL multiple times.
        /// </summary>
        [Test]
        public void TestDisableWalReturnsFalseIfWalWasDisabledBefore()
        {
            var clientCluster = GetCluster();
            clientCluster.EnableWal(PersistentCache);

            Assert.IsTrue(clientCluster.DisableWal(PersistentCache));
            Assert.IsFalse(clientCluster.DisableWal(PersistentCache));
        }

        /// <summary>
        /// Test disable WAL validates cache name.
        /// </summary>
        [TestCase(null)]
        [TestCase("")]
        public void TestDisableWalValidatesCacheNameArgument(string cacheName)
        {
            Assert.Throws<ArgumentException>(() => GetCluster().DisableWal(cacheName),
                "'cacheName' argument should not be null or empty.");
        }

        /// <summary>
        /// Returns Ignite cluster.
        /// </summary>
        private IClientCluster GetCluster()
        {
            return Client.GetCluster();
        }
    }
}
