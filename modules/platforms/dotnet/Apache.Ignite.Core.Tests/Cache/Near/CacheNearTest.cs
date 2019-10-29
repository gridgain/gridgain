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
        protected const string DefaultCacheName = "default";

        /** */
        private IIgnite _grid;

        /** */
        private IIgnite _grid2;

        /** */
        private IIgnite _client;
        
        /** */
        private volatile CacheEvent _lastEvent;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public virtual void FixtureSetUp()
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
                IgniteInstanceName = "server1"
            };

            _grid = Ignition.Start(cfg);
            
            var cfg2 = new IgniteConfiguration(cfg)
            {
                IgniteInstanceName = "server2"
            };

            _grid2 = Ignition.Start(cfg2);

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
        public void TestNearCachePutGetReturnsSameObjectReference(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cache = GetCache<int, Foo>(mode);
            var key = (int) mode;

            var obj1 = new Foo(key);
            
            cache[key] = obj1;
            var res1 = cache[key];

            Assert.AreSame(obj1, res1);
        }

        /// <summary>
        /// Tests that near cache returns the same object on every get.
        /// </summary>
        [Test]
        public void TestNearCacheRepeatedGetReturnsSameObjectReference(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cache = GetCache<int, Foo>(mode);
            var key = TestUtils.GetPrimaryKey(_grid, cache.Name);

            var obj = new Foo();
            
            cache[key] = obj;
            
            var res1 = cache[key];
            var res2 = cache[key];
            
            Assert.AreSame(res1, res2);
        }
        
        /// <summary>
        /// Tests that near cache returns the same object on every get.
        /// </summary>
        [Test]
        public void TestNearCacheRepeatedRemoteGetReturnsSameObjectReference(
            [Values(CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var remoteCache = GetCache<int, Foo>(CacheTestMode.ServerLocal);
            var localCache = GetCache<int, Foo>(mode);
            var key = TestUtils.GetPrimaryKey(_grid, remoteCache.Name);

            remoteCache[key] = new Foo();

            Assert.IsTrue(TestUtils.WaitForCondition(() =>
            {
                var res1 = localCache.Get(key);
                var res2 = localCache.Get(key);

                return ReferenceEquals(res1, res2);
            }, 300));
            
            // Invalidate after get.
            remoteCache[key] = new Foo(1);
            
            Assert.IsTrue(TestUtils.WaitForCondition(() =>
            {
                var res1 = localCache.Get(key);
                var res2 = localCache.Get(key);

                return res1.Bar == 1 && ReferenceEquals(res1, res2);
            }, 300));
        }
        
        /// <summary>
        /// Tests that near cache is updated from remote node after being populated with local Put call.
        /// </summary>
        [Test]
        public void TestNearCacheUpdatesFromRemoteNode(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode1,
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode2)
        {
            var cache1 = GetCache<int, int>(mode1);
            var cache2 = GetCache<int, int>(mode2);

            cache1[1] = 1;
            cache2[1] = 2;

            Assert.True(TestUtils.WaitForCondition(() => cache1[1] == 2, 300));
        }

        /// <summary>
        /// Tests that near cache is updated from another cache instance after being populated with local Put call.
        /// </summary>
        [Test]
        public void TestNearCacheUpdatesFromAnotherLocalInstance()
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
            var cache3 = _grid.GetCache<int, Foo>(DefaultCacheName);
            var cache4 = _grid.GetCache<object, object>(DefaultCacheName);

            cache1[1] = 1;
            cache2["1"] = "1";
            cache3[2] = new Foo(5);

            Assert.AreEqual(cache4[1], 1);
            Assert.AreEqual(cache4["1"], "1");
            Assert.AreSame(cache4[2], cache3[2]);
        }

        /// <summary>
        /// Tests that near cache data is cleared when underlying cache is destroyed.
        /// </summary>
        [Test]
        public void TestDestroyCacheClearsNearCacheData(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cfg = new CacheConfiguration
            {
                Name = "destroy-test",
                NearConfiguration = new NearCacheConfiguration()
            };

            var ignite = GetIgnite(mode);
            
            var cache = ignite.CreateCache<int, int>(cfg, new NearCacheConfiguration());
            
            cache[1] = 1;
            ignite.DestroyCache(cache.Name);

            var ex = Assert.Throws<InvalidOperationException>(() => cache.Get(1));
            
            // TODO: why does message start with class o.a.i...?
            StringAssert.EndsWith(
                "Failed to perform cache operation (cache is stopped): destroy-test", 
                ex.Message);
        }

        /// <summary>
        /// Tests that error during Put removes near cache value for that key.
        /// </summary>
        [Test]
        public void TestFailedPutRemovesNearCacheValue()
        {
            // TODO: use store to cause error during put
        }

        /// <summary>
        /// Tests that near cache is updated/invalidated by SQL DML operations.
        /// </summary>
        [Test]
        public void TestSqlUpdatesNearCache()
        {
            // TODO
        }

        /// <summary>
        /// Tests that eviction policy removes near cache data for the key. 
        /// </summary>
        [Test]
        public void TestEvictionPolicyRemovesNearCacheValue()
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
        
        /// <summary>
        /// Gets the cache instance.
        /// </summary>
        private ICache<TK, TV> GetCache<TK, TV>(CacheTestMode mode, string name = DefaultCacheName)
        {
            switch (mode)
            {
                case CacheTestMode.ServerLocal:
                    return _grid.GetCache<TK, TV>(name);
                
                case CacheTestMode.ServerRemote:
                    return _grid2.GetCache<TK, TV>(name);
                
                case CacheTestMode.Client:
                    return _client.GetOrCreateNearCache<TK, TV>(name, new NearCacheConfiguration());
                
                default:
                    throw new ArgumentOutOfRangeException("mode", mode, null);
            }
        }

        /// <summary>
        /// Gets Ignite instance for mode.
        /// </summary>
        private IIgnite GetIgnite(CacheTestMode mode)
        {
            return new[] {_grid, _grid2, _client}[(int) mode];
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
            public Foo(int bar = 0)
            {
                Bar = bar;
            }

            public readonly int Bar;

            public readonly string TestName = TestContext.CurrentContext.Test.Name;

            public override string ToString()
            {
                return string.Format("Foo [Bar={0}, TestName={1}]", Bar, TestName);
            }
        }
        
        /** */
        public enum CacheTestMode
        {
            ServerLocal,
            ServerRemote,
            Client
        }
    }
}
