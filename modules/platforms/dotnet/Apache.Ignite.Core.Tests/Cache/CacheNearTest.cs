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
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Events;
    using NUnit.Framework;

    /// <summary>
    /// Java-based (legacy) near cache test.
    /// See also <see cref="CacheWithNearCacheTest"/>.
    /// </summary>
    public class CacheNearTest : IEventListener<CacheEvent>
    {
        /** */
        private const string DefaultCacheName = "default";

        /** */
        private IIgnite _grid;

        /** */
        private volatile CacheEvent _lastEvent;

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
        /// Tests the existing near cache.
        /// </summary>
        [Test]
        public void TestExistingNearCache()
        {
            var cache = _grid.GetCache<int, string>(DefaultCacheName);
            cache[1] = "1";

            var nearCache = _grid.GetOrCreateNearCache<int, string>(DefaultCacheName, new NearCacheConfiguration());
            Assert.AreEqual("1", nearCache[1]);

            // GetOrCreate when exists
            nearCache = _grid.GetOrCreateNearCache<int, string>(DefaultCacheName, new NearCacheConfiguration());
            Assert.AreEqual("1", nearCache[1]);

            cache[1] = "2";
            Assert.AreEqual("2", nearCache[1]);
        }

        /// <summary>
        /// Tests the created near cache.
        /// </summary>
        [Test]
        public void TestCreateNearCache(
            [Values(/* TODO: CacheMode.Local,*/ CacheMode.Partitioned, CacheMode.Replicated)] CacheMode cacheMode,
            [Values(CacheAtomicityMode.Atomic, CacheAtomicityMode.Transactional)] CacheAtomicityMode atomicityMode)
        {
            var cacheName = string.Format("dyn_cache_{0}_{1}", cacheMode, atomicityMode);

            var cfg = new CacheConfiguration(cacheName)
            {
                AtomicityMode = atomicityMode,
                CacheMode = cacheMode
            };
            
            var cache = _grid.CreateCache<int, string>(cfg);
            cache[1] = "1";

            using (var client = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "client"
            }))
            {
                var nearCache = client.CreateNearCache<int, string>(cacheName, new NearCacheConfiguration());
                Assert.AreEqual("1", nearCache[1]);

                // Create when exists.
                nearCache = client.CreateNearCache<int, string>(cacheName, new NearCacheConfiguration());
                Assert.AreEqual("1", nearCache[1]);

                // Update entry.
                cache[1] = "2";
                Thread.Sleep(1000);  // TODO: Not good
                Assert.AreEqual("2", nearCache[1]);
                
                // Update through near.
                nearCache[1] = "3";
                Assert.AreEqual("3", nearCache[1]);
                
                // Remove.
                cache.Clear(1);
                Thread.Sleep(1000); // TODO
                Assert.False(cache.ContainsKey(1));
                
                // TODO
                // Assert.False(nearCache.ContainsKey(1));
            }
        }

        /// <summary>
        /// Tests near cache on the client node.
        /// </summary>
        [Test]
        public void TestCreateNearCacheOnClientNode()
        {
            const string cacheName = "client_cache";

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "clientGrid"
            };

            using (var clientGrid = Ignition.Start(cfg))
            {
                clientGrid.CreateCache<int, string>(cacheName);

                // Near cache can't be started on client node
                Assert.Throws<CacheException>(
                    () => clientGrid.CreateNearCache<int, string>(cacheName, new NearCacheConfiguration()));
            }
        }

        /// <summary>
        /// Tests near cache on the client node.
        /// </summary>
        [Test]
        public void TestCreateCacheWithNearConfigOnClientNode()
        {
            const string cacheName = "client_with_near_cache";

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "clientGrid",
                IncludedEventTypes = new[] {EventType.CacheEntryCreated}
            };

            using (var clientGrid = Ignition.Start(cfg))
            {
                var cache = clientGrid.CreateCache<int, string>(new CacheConfiguration(cacheName), 
                    new NearCacheConfiguration());

                AssertCacheIsNear(cache);

                cache[1] = "1";
                Assert.AreEqual("1", cache[1]);

                var cache2 = clientGrid.GetOrCreateCache<int, string>(new CacheConfiguration(cacheName),
                    new NearCacheConfiguration());

                Assert.AreEqual("1", cache2[1]);

                cache[1] = "2";
                Assert.AreEqual("2", cache2[1]);
            }
        }
        
        /// <summary>
        /// Tests that near cache returns the same object instance as we put there.
        /// </summary>
        [Test]
        public void TestNearCacheReturnsSameObjectReference()
        {
            var cache = _grid.GetCache<int, Foo>(DefaultCacheName);

            var obj1 = new Foo(12);
            var obj2 = new Foo(42);
            
            cache[1] = obj1;
            var res1 = cache[1];

            cache[1] = obj2;
            var res2 = cache[1];
            
            Assert.AreSame(obj1, res1);
            Assert.AreSame(obj2, res2);
        }
        
        /// <summary>
        /// Tests that near cache is updated from remote node after being populated with local Put call.
        /// </summary>
        [Test]
        public void TestNearCacheUpdatesFromRemoteNodeAfterLocalPut()
        {
            // TODO:
        }

        /// <summary>
        /// Tests that near cache is cleared from remote node after being populated with local Put call.
        /// </summary>
        [Test]
        public void TestNearCacheClearFromRemoteNodeAfterLocalPut()
        {
            // TODO
        }

        /// <summary>
        /// Asserts the cache is near.
        /// </summary>
        private void AssertCacheIsNear(ICache<int, string> cache)
        {
            var events = cache.Ignite.GetEvents();
            events.LocalListen(this, EventType.CacheEntryCreated);

            _lastEvent = null;
            cache[-1] = "test";

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
            public Foo(int val)
            {
                Val = val;
            }

            public int Val;
        }
    }
}
