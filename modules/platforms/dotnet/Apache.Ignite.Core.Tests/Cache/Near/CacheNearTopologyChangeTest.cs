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

namespace Apache.Ignite.Core.Tests.Cache.Near
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests Near Cache behavior when cluster topology changes.
    /// </summary>
    public class CacheNearTopologyChangeTest
    {
        /** */
        private const string CacheName = "c";

        /** Key that is primary on node3 */
        private const int Key3 = 6;

        /** */
        private IIgnite[] _ignite;

        /** */
        private ICache<int, Foo>[] _cache;

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }
        
        /// <summary>
        /// Tests that near cache is cleared when any server node leaves.
        /// </summary>
        [Test]
        public void TestServerNodeLeaveClearsNearCache()
        {
            InitGrids(3);
            var key = Key3;

            _cache[0][key] = new Foo(key);

            Assert.AreSame(_cache[0].Get(key), _cache[0].Get(key));
            Assert.AreSame(_cache[1].Get(key), _cache[1].Get(key));
            Assert.AreEqual(key, _cache[0][key].Bar);
            Assert.AreEqual(key, _cache[1][key].Bar);

            _ignite[2].Dispose();
            Assert.IsTrue(_ignite[0].WaitTopology(2));

            // Check that key is not stuck in near cache.
            Assert.IsEmpty(_cache[0].GetAll(new[] {key}));
            Assert.IsEmpty(_cache[1].GetAll(new[] {key}));
            Assert.Throws<KeyNotFoundException>(() => _cache[0].Get(key));
            Assert.Throws<KeyNotFoundException>(() => _cache[1].Get(key));
            
            // Check that updates for that key work on both nodes.
            _cache[0][key] = new Foo(1);
            Assert.AreEqual(1, _cache[0][key].Bar);
            Assert.AreEqual(1, _cache[1][key].Bar);
            Assert.AreSame(_cache[0][key], _cache[0][key]);
            Assert.AreSame(_cache[1][key], _cache[1][key]);
            
            _cache[1][key] = new Foo(2);
            Assert.IsTrue(TestUtils.WaitForCondition(() => _cache[0][key].Bar == 2, 500));
            Assert.AreEqual(2, _cache[0][key].Bar);
            Assert.AreEqual(2, _cache[1][key].Bar);
            Assert.AreSame(_cache[0][key], _cache[0][key]);
            Assert.AreSame(_cache[1][key], _cache[1][key]);
        }

        [Test]
        public void TestServerNodeBecomesNoLongerPrimaryKeepsNearCacheData()
        {
            
            // TODO: test that near invalidation still works after primary change
            // Especially when on Server node we had NearCacheEntry and then it changes to normal entry, and vice versa
        }

        /// <summary>
        /// Inits a number of grids.
        /// </summary>
        private void InitGrids(int count)
        {
            _ignite = new IIgnite[count];
            _cache = new ICache<int, Foo>[count];

            for (var i = 0; i < count; i++)
            {
                InitGrid(i);
            }
        }

        /// <summary>
        /// Inits a grid.
        /// </summary>
        private void InitGrid(int i)
        {
            var cacheConfiguration = new CacheConfiguration(CacheName)
            {
                NearConfiguration = new NearCacheConfiguration()
            };
            
            _ignite[i] = Ignition.Start(TestUtils.GetTestConfiguration("node" + i));
            _cache[i] = _ignite[i].GetOrCreateCache<int, Foo>(cacheConfiguration);
            
            if (i == 2)
            {
                // ReSharper disable once AccessToDisposedClosure
                Assert.IsTrue(
                    TestUtils.WaitForCondition(() => TestUtils.GetPrimaryKey(_ignite[2], CacheName) == Key3, 3000));
            }
        }
    }
}