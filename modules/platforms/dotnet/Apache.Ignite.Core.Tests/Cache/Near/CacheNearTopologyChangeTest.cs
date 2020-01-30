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
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Events;
    using NUnit.Framework;

    /// <summary>
    /// Tests Near Cache behavior when cluster topology changes.
    /// </summary>
    public class CacheNearTopologyChangeTest
    {
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
            var grid1 = Ignition.Start(TestUtils.GetTestConfiguration());
            var grid2 = Ignition.Start(TestUtils.GetTestConfiguration(name: "node2"));

            var cacheConfiguration = new CacheConfiguration("c") {NearConfiguration = new NearCacheConfiguration()};
            var cache = grid1.CreateCache<int, Foo>(cacheConfiguration);
            
            var key = TestUtils.GetPrimaryKey(grid2, cache.Name);
            cache[key] = new Foo(key);

            Assert.AreSame(cache.Get(key), cache.Get(key), "key is in near cache on grid1");

            grid2.Dispose();
            Assert.IsTrue(grid1.WaitTopology(1));

            Assert.IsEmpty(cache.GetAll(new[] {key}), "key is removed from near cache");
            Assert.Throws<KeyNotFoundException>(() => cache.Get(key), "key is removed from near cache");
        }

        /// <summary>
        /// Tests that near cache data is retained and keeps updating properly when current server node becomes primary
        /// for a given key after being non-primary (GridNearCacheEntry -> GridDhtCacheEntry). 
        /// </summary>
        [Test]
        public void TestServerNodeBecomesPrimaryKeepsNearCacheData()
        {
            var grid1 = Ignition.Start(TestUtils.GetTestConfiguration("node1"));
            var grid2 = Ignition.Start(TestUtils.GetTestConfiguration(name: "node2"));
            
            var cacheConfiguration = new CacheConfiguration("c") {NearConfiguration = new NearCacheConfiguration()};
            var cache = grid1.CreateCache<int, Foo>(cacheConfiguration);
            
            Thread.Sleep(300);

            // Grid1: 2, 3, 9, 10, 11, 13, 18, 19, 20, 22
            // Grid2: 1, 4, 5, 6, 7, 8, 12, 14, 15, 16
            Console.WriteLine("Grid1:" + TestUtils.GetPrimaryKeys(grid1, cache.Name)
                .Select(x => x.ToString()).Take(10).Aggregate((a, b) => a + ", " + b));
            
            Console.WriteLine("Grid2:" + TestUtils.GetPrimaryKeys(grid2, cache.Name)
                .Select(x => x.ToString()).Take(10).Aggregate((a, b) => a + ", " + b));
            
            
            var grid3 = Ignition.Start(TestUtils.GetTestConfiguration(name: "node3"));

            cache[1] = new Foo(111);
            Thread.Sleep(3000);
            
            // Grid1: 2, 3, 9, 10, 11, 13, 19, 20, 25, 29
            // Grid2: 1, 4, 5, 7, 8, 12, 15, 16, 21, 24
            // Grid3: 6, 14, 17, 18, 22, 23, 26, 27, 30, 36

            Console.WriteLine("Grid1:" + TestUtils.GetPrimaryKeys(grid1, cache.Name)
                                  .Select(x => x.ToString()).Take(10).Aggregate((a, b) => a + ", " + b));
            
            Console.WriteLine("Grid2:" + TestUtils.GetPrimaryKeys(grid2, cache.Name)
                                  .Select(x => x.ToString()).Take(10).Aggregate((a, b) => a + ", " + b));
            
            Console.WriteLine("Grid3:" + TestUtils.GetPrimaryKeys(grid3, cache.Name)
                                  .Select(x => x.ToString()).Take(10).Aggregate((a, b) => a + ", " + b));
        }
        
        [Test]
        public void TestServerNodeNoLongerPrimaryKeepsNearCacheData()
        {
            // Key that becomes primary on node3 when it enters topology. 
            const int key = 6;

            // TODO: test that near invalidation still works after primary change
            // Especially when on Server node we had NearCacheEntry and then it changes to normal entry, and vice versa
        }
    }
}