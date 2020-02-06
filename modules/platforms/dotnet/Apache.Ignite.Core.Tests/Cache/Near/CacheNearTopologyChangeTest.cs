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
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
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
        private const int MaxGrids = 10;

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

            _cache[0][Key3] = new Foo(Key3);

            Assert.AreSame(_cache[0].Get(Key3), _cache[0].Get(Key3));
            Assert.AreSame(_cache[1].Get(Key3), _cache[1].Get(Key3));
            Assert.AreEqual(Key3, _cache[0][Key3].Bar);
            Assert.AreEqual(Key3, _cache[1][Key3].Bar);

            _ignite[2].Dispose();
            Assert.IsTrue(_ignite[0].WaitTopology(2));

            // Check that key is not stuck in near cache.
            TestUtils.WaitForCondition(() => !_cache[0].ContainsKey(Key3), 1000);
            Assert.Throws<KeyNotFoundException>(() => _cache[0].Get(Key3));
            Assert.Throws<KeyNotFoundException>(() => _cache[1].Get(Key3));
            
            // Check that updates for that key work on both nodes.
            _cache[0][Key3] = new Foo(1);
            Assert.AreEqual(1, _cache[0][Key3].Bar);
            Assert.AreEqual(1, _cache[1][Key3].Bar);
            Assert.AreSame(_cache[0][Key3], _cache[0][Key3]);
            Assert.AreSame(_cache[1][Key3], _cache[1][Key3]);
            
            _cache[1][Key3] = new Foo(2);
            Assert.IsTrue(TestUtils.WaitForCondition(() => _cache[0][Key3].Bar == 2, 500));
            Assert.AreEqual(2, _cache[0][Key3].Bar);
            Assert.AreEqual(2, _cache[1][Key3].Bar);
            Assert.AreSame(_cache[0][Key3], _cache[0][Key3]);
            Assert.AreSame(_cache[1][Key3], _cache[1][Key3]);
        }

        /// <summary>
        /// Tests that near cache works correctly after primary node changes for a given key.
        /// GridNearCacheEntry -> GridDhtCacheEntry.
        /// </summary>
        [Test]
        public void TestPrimaryNodeChangeClearsNearCacheDataOnServer()
        {
            InitGrids(2);
            
            _cache[0][Key3] = new Foo(-1);
            for (var i = 0; i < 2; i++)
            {
                // ReSharper disable once AccessToModifiedClosure
                Assert.IsTrue(TestUtils.WaitForCondition(() => _cache[i][Key3].Bar == -1, 1000));
            }

            // New node enters and becomes primary for the key.
            InitGrid(2);
            
            // GridCacheNearEntry does not yet exist on old primary node, so near cache data is removed on .NET side.
            Foo foo;
            Assert.IsFalse(_cache[0].TryLocalPeek(Key3, out foo, CachePeekMode.NativeNear));
            Assert.IsFalse(_cache[1].TryLocalPeek(Key3, out foo, CachePeekMode.NativeNear));
            
            // Check value on the new node.
            Assert.AreEqual(-1, _cache[2][Key3].Bar);
            Assert.AreSame(_cache[2][Key3], _cache[2][Key3]);
            
            // Check that updates are propagated to all server nodes.
            _cache[2][Key3] = new Foo(3);
            
            for (var i = 0; i < 3; i++)
            {
                // ReSharper disable once AccessToModifiedClosure
                Assert.IsTrue(TestUtils.WaitForCondition(() => _cache[i][Key3].Bar == 3, 1000));
                Assert.AreSame(_cache[i][Key3], _cache[i][Key3]);
            }
        }        
        
        /// <summary>
        /// Tests that near cache works correctly on client node after primary node changes for a given key.
        /// </summary>
        [Test]
        public void TestPrimaryNodeChangeClearsNearCacheDataOnClient()
        {
            InitGrids(2, serverNear: false);
            var clientCache = InitClientAndCache();

            _cache[0][Key3] = new Foo(-1);

            var clientInstance = clientCache[Key3];
            Assert.AreEqual(-1, clientInstance.Bar);
            
            // New node enters and becomes primary for the key.
            InitGrid(2);
            
            // Client node cache is cleared.
            Foo foo;
            Assert.IsFalse(clientCache.TryLocalPeek(Key3, out foo, CachePeekMode.NativeNear));
            
            // Updates are propagated to client near cache.
            _cache[2][Key3] = new Foo(3);
            
            Assert.IsTrue(TestUtils.WaitForCondition(() => clientCache[Key3].Bar == 3, 1000));
            Assert.IsTrue(clientCache.TryLocalPeek(Key3, out foo, CachePeekMode.NativeNear));
            Assert.AreNotSame(clientInstance, foo);
            Assert.AreEqual(3, foo.Bar);
        }

        /// <summary>
        /// Tests that client node entering topology does not cause any near caches invalidation.
        /// </summary>
        [Test]
        public void TestClientNodeJoinOrLeaveDoesNotAffectNearCacheDataOnOtherNodes()
        {
            InitGrids(2);
            _cache[2] = InitClientAndCache();

            Action<Action<ICache<int, Foo>, int>> forEachCacheAndKey = (act) =>
            {
                for (var gridIdx = 0; gridIdx < 3; gridIdx++)
                {
                    var cache = _cache[gridIdx];

                    for (var key = 0; key < 100; key++)
                    {
                        act(cache, key);
                    }
                }
            };

            Action<int> putData = offset => forEachCacheAndKey((cache, key) => cache[key] = new Foo(key + offset));
            
            Action<int> checkNearData = offset => forEachCacheAndKey((cache, key) => 
                Assert.AreEqual(key + offset, cache.LocalPeek(key, CachePeekMode.NativeNear).Bar));

            // Put data and verify near cache.
            putData(0);
            checkNearData(0);

            // Start new client node, check near data, stop client node, check near data.
            using (InitClient())
            {
                checkNearData(0);
                putData(1);
                checkNearData(1);
            }

            checkNearData(1);
            putData(2);
            checkNearData(2);
            
            Assert.AreEqual(3, _cache[0][1].Bar);
        }

        /// <summary>
        /// Test multiple topology changes.
        /// </summary>
        [Test]
        public void TestContinuousTopologyChangeMaintainsCorrectNearCacheData()
        {
            // TODO: stop/start random nodes, put/get random keys, verify on all caches
        }

        /// <summary>
        /// Inits a number of grids.
        /// </summary>
        private void InitGrids(int count, bool serverNear = true)
        {
            Debug.Assert(count < MaxGrids);
            
            _ignite = new IIgnite[MaxGrids];
            _cache = new ICache<int, Foo>[MaxGrids];

            for (var i = 0; i < count; i++)
            {
                InitGrid(i, serverNear);
            }
        }

        /// <summary>
        /// Inits a grid.
        /// </summary>
        private void InitGrid(int i, bool serverNear = true)
        {
            var cacheConfiguration = new CacheConfiguration(CacheName)
            {
                NearConfiguration = serverNear ? new NearCacheConfiguration() : null
            };
            
            _ignite[i] = Ignition.Start(TestUtils.GetTestConfiguration(name: "node" + i));
            _cache[i] = _ignite[i].GetOrCreateCache<int, Foo>(cacheConfiguration);
            
            if (i == 2)
            {
                // ReSharper disable once AccessToDisposedClosure
                Assert.IsTrue(
                    TestUtils.WaitForCondition(() => TestUtils.GetPrimaryKey(_ignite[2], CacheName) == Key3, 3000));
            }
        }
        
        /// <summary>
        /// Inits a client node and a near cache on it.
        /// </summary>
        private static ICache<int, Foo> InitClientAndCache()
        {
            var client = InitClient();

            return client.CreateNearCache<int, Foo>(CacheName, new NearCacheConfiguration());
        }

        /// <summary>
        /// Inits a client node.
        /// </summary>
        private static IIgnite InitClient()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(name: "client" + Guid.NewGuid()))
            {
                ClientMode = true
            };

            return Ignition.Start(cfg);
        }
    }
}