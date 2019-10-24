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

namespace Apache.Ignite.Core.Tests.Cache
{
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Events;
    using NUnit.Framework;

    /// <summary>
    /// Near cache test.
    /// </summary>
    public class CacheNearTest : IEventListener<CacheEvent>
    {
        /** */
        private const string DefaultCacheName = "default";

        /** */
        private IIgnite _grid;

        /** */
        private volatile CacheEvent _lastEvent;

        /** */
        private IIgnite _client;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        NearConfiguration = new NearCacheConfiguration
                        {
                            EvictionPolicy = new FifoEvictionPolicy {MaxSize = 5}
                        },
                        Name = DefaultCacheName
                    }
                },
                IncludedEventTypes = new[] { EventType.CacheEntryCreated },
                IgniteInstanceName = "server"
            };

            _grid = Ignition.Start(cfg);

            var clientCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "client",
                IncludedEventTypes = new[] {EventType.CacheEntryCreated}
            };

            _client = Ignition.Start(clientCfg);
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Test tear down.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            // _grid.GetCache<int, int>(DefaultCacheName).RemoveAll();
        }

        /// <summary>
        /// Tests the existing near cache.
        /// </summary>
        [Test]
        public void TestExistingNearCache()
        {
            var cache = _grid.GetCache<int, int>(DefaultCacheName);
            cache[1] = 1;

            var nearCache = _grid.GetOrCreateNearCache<int, int>(DefaultCacheName, new NearCacheConfiguration());
            Assert.AreEqual(1, nearCache[1]);

            // GetOrCreate when exists
            nearCache = _grid.GetOrCreateNearCache<int, int>(DefaultCacheName, new NearCacheConfiguration());
            Assert.AreEqual(1, nearCache[1]);

            cache[1] = 2;
            Assert.AreEqual(2, nearCache[1]);
        }

        /// <summary>
        /// Tests the created near cache.
        /// </summary>
        [Test]
        public void TestCreateNearCache(
            [Values( /* TODO: CacheMode.Local,*/ CacheMode.Partitioned, CacheMode.Replicated)]
            CacheMode cacheMode,
            [Values(CacheAtomicityMode.Atomic, CacheAtomicityMode.Transactional)]
            CacheAtomicityMode atomicityMode)
        {
            var cacheName = string.Format("dyn_cache_{0}_{1}", cacheMode, atomicityMode);

            var cfg = new CacheConfiguration(cacheName)
            {
                AtomicityMode = atomicityMode,
                CacheMode = cacheMode
            };

            var cache = _grid.CreateCache<int, int>(cfg);
            cache[1] = 1;

            var nearCache = _client.CreateNearCache<int, int>(cacheName, new NearCacheConfiguration());
            Assert.AreEqual(1, nearCache[1]);

            // Create when exists.
            nearCache = _client.CreateNearCache<int, int>(cacheName, new NearCacheConfiguration());
            Assert.AreEqual(1, nearCache[1]);

            // Update entry.
            cache[1] = 2;
            Assert.True(TestUtils.WaitForCondition(() => nearCache[1] == 2, 300));

            // Update through near.
            nearCache[1] = 3;
            Assert.AreEqual(3, nearCache[1]);

            // Remove.
            cache.Remove(1);
            Assert.True(TestUtils.WaitForCondition(() => !nearCache.ContainsKey(1), 300));
        }

        /// <summary>
        /// Tests near cache on the client node.
        /// </summary>
        [Test]
        public void TestCreateNearCacheOnClientNode()
        {
            const string cacheName = "client_cache";

            _client.CreateCache<int, int>(cacheName);

            // Near cache can't be started on client node
            Assert.Throws<CacheException>(
                () => _client.CreateNearCache<int, int>(cacheName, new NearCacheConfiguration()));
        }

        /// <summary>
        /// Tests near cache on the client node.
        /// </summary>
        [Test]
        public void TestCreateCacheWithNearConfigOnClientNode()
        {
            const string cacheName = "client_with_near_cache";

            var cache = _client.CreateCache<int, int>(new CacheConfiguration(cacheName),
                new NearCacheConfiguration());

            AssertCacheIsNear(cache);

            cache[1] = 1;
            Assert.AreEqual(1, cache[1]);

            var cache2 = _client.GetOrCreateCache<int, int>(new CacheConfiguration(cacheName),
                new NearCacheConfiguration());

            Assert.AreEqual(1, cache2[1]);

            cache[1] = 2;
            Assert.AreEqual(2, cache2[1]);
        }

        /// <summary>
        /// Tests that near cache returns the same object instance as we put there.
        /// </summary>
        [Test]
        public void TestNearCachePutGetReturnsSameObjectReference()
        {
            var cache = _grid.GetCache<int, Foo>(DefaultCacheName);

            var obj1 = new Foo();
            var obj2 = new Foo();
            
            cache[1] = obj1;
            var res1 = cache[1];

            cache[1] = obj2;
            var res2 = cache[1];
            
            Assert.AreSame(obj1, res1);
            Assert.AreSame(obj2, res2);
        }

        /// <summary>
        /// Tests that near cache returns the same object instance as we put there.
        /// </summary>
        [Test]
        public void TestNearCacheRepeatedGetReturnsSameObjectReference()
        {
            var cache = _grid.GetCache<int, Foo>(DefaultCacheName);

            var obj = new Foo();
            
            cache[1] = obj;
            
            var res1 = cache[1];
            var res2 = cache[1];
            
            Assert.AreSame(res1, res2);
        }
        
        /// <summary>
        /// Tests that near cache is updated from remote node after being populated with local Put call.
        /// </summary>
        [Test]
        public void TestNearCacheUpdatesFromRemoteNodeAfterLocalPut()
        {
            var localCache = _client.GetOrCreateNearCache<int, int>(DefaultCacheName, new NearCacheConfiguration());
            var remoteCache = _grid.GetCache<int, int>(DefaultCacheName);

            localCache[1] = 1;
            remoteCache[1] = 2;

            Assert.True(TestUtils.WaitForCondition(() => localCache[1] == 2, 300));
        }

        /// <summary>
        /// Tests that near cache is updated from another cache instance after being populated with local Put call.
        /// </summary>
        [Test]
        public void TestNearCacheUpdatesAfterLocalPut()
        {
            var cache1 = _grid.GetCache<int, int>(DefaultCacheName);
            var cache2 = _grid.GetCache<int, int>(DefaultCacheName);

            cache1[1] = 1;
            cache2.Replace(1, 2);

            Assert.True(TestUtils.WaitForCondition(() => cache1[1] == 2, 300));
        }

        /// <summary>
        /// Tests that near cache is cleared from remote node after being populated with local Put call.
        /// </summary>
        [Test]
        public void TestNearCacheRemoveFromRemoteNodeAfterLocalPut()
        {
            var localCache = _client.GetOrCreateNearCache<int, int>(DefaultCacheName, new NearCacheConfiguration());
            var remoteCache = _grid.GetCache<int, int>(DefaultCacheName);

            localCache[1] = 1;
            remoteCache.Remove(1);

            int unused;
            Assert.True(TestUtils.WaitForCondition(() => !localCache.TryGet(1, out unused), 300));
        }

        /// <summary>
        /// Tests that same near cache can be used with different sets of generic type parameters.
        /// </summary>
        [Test]
        public void TestSameNearCacheWithDifferentGenericTypeParameters()
        {
            var cache1 = _grid.GetCache<int, int>(DefaultCacheName);
            var cache2 = _grid.GetCache<string, string>(DefaultCacheName);
            var cache3 = _grid.GetCache<object, object>(DefaultCacheName);

            cache1[1] = 1;
            cache2["1"] = "1";

            Assert.AreEqual(cache3[1], 1);
            Assert.AreEqual(cache3["1"], "1");
        }

        /// <summary>
        /// Tests that near cache data is cleared when underlying cache is destroyed.
        /// </summary>
        [Test]
        public void TestDestroyCacheClearsNearCacheData()
        {
            // TODO
        }

        /// <summary>
        /// Asserts the cache is near.
        /// </summary>
        private void AssertCacheIsNear(ICache<int, int> cache)
        {
            var events = cache.Ignite.GetEvents();
            events.LocalListen(this, EventType.CacheEntryCreated);

            _lastEvent = null;
            cache[-1] = int.MinValue;

            TestUtils.WaitForCondition(() => _lastEvent != null, 500);
            Assert.IsNotNull(_lastEvent);
            Assert.IsTrue(_lastEvent.IsNear);

            events.StopLocalListen(this, EventType.CacheEntryCreated);
        }

        /** <inheritdoc /> */
        public bool Invoke(CacheEvent evt)
        {
            _lastEvent = evt;
            return true;
        }
        
        /** */
        private class Foo
        {
            // No-op.
        }
    }
}
