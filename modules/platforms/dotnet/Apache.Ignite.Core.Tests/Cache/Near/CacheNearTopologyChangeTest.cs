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
            var cache1 = grid1.CreateCache<int, Foo>(cacheConfiguration);
            var cache2 = grid2.GetCache<int, Foo>(cache1.Name);
            
            var key = TestUtils.GetPrimaryKey(grid2, cache1.Name);
            cache1[key] = new Foo(key);

            Assert.AreSame(cache1.Get(key), cache1.Get(key), "key is in near cache on grid1");

            grid2.Dispose();
            Assert.IsTrue(grid1.WaitTopology(1));

            // Check that key is not stuck in near cache.
            Assert.IsEmpty(cache1.GetAll(new[] {key}));
            Assert.IsEmpty(cache2.GetAll(new[] {key}));
            Assert.Throws<KeyNotFoundException>(() => cache1.Get(key));
            Assert.Throws<KeyNotFoundException>(() => cache2.Get(key));
            
            // Check that updates for that key work on both nodes.
            cache1[key] = new Foo(1);
            Assert.AreEqual(1, cache2[key].Bar);
            Assert.AreSame(cache2[key], cache2[key]);
            
            cache2[key] = new Foo(2);
            Assert.AreEqual(2, cache1[key].Bar);
            Assert.AreSame(cache1[key], cache1[key]);
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

            // Grid1:2, 3, 9, 10, 11, 13, 18, 19, 20, 22, 25, 29, 32, 38, 42, 44, 48, 51, 52, 53, 57, 65, 69, 72, 75, 77, 79, 81, 82, 87, 89, 90, 95, 96, 97, 98, 100, 101, 103, 104, 106, 112, 114, 116, 121, 122, 123, 125, 127, 128
            // Grid2:1, 4, 5, 6, 7, 8, 12, 14, 15, 16, 17, 21, 23, 24, 26, 27, 28, 30, 31, 33, 34, 35, 36, 37, 39, 40, 41, 43, 45, 46, 47, 49, 50, 54, 55, 56, 58, 59, 60, 61, 62, 63, 64, 66, 67, 68, 70, 71, 73, 74

            Console.WriteLine("Grid1:" + TestUtils.GetPrimaryKeys(grid1, cache.Name)
                                  .Select(x => x.ToString()).Take(50).Aggregate((a, b) => a + ", " + b));
            
            Console.WriteLine("Grid2:" + TestUtils.GetPrimaryKeys(grid2, cache.Name)
                .Select(x => x.ToString()).Take(50).Aggregate((a, b) => a + ", " + b));
            
            
            var grid3 = Ignition.Start(TestUtils.GetTestConfiguration(name: "node3"));

            cache[1] = new Foo(111);
            Thread.Sleep(3000);
            
            // Grid1: 2, 3, 9, 10, 11, 13, 19, 20, 25, 29, 32, 38, 42, 48, 52, 53, 65, 69, 72, 77, 79, 81, 82, 87, 89, 96, 97, 100, 104, 116, 121, 122, 123, 127, 132, 150, 151, 152, 157, 158, 160, 164, 165, 168, 175, 176, 180, 182, 185, 186
            // Grid2: 1, 4, 5, 7, 8, 12, 15, 16, 21, 24, 28, 31, 33, 34, 35, 40, 43, 46, 50, 54, 55, 56, 61, 63, 64, 66, 70, 74, 78, 80, 84, 85, 86, 92, 93, 99, 102, 107, 108, 109, 110, 111, 113, 115, 119, 120, 124, 129, 130, 134
            // Grid3: 6, 14, 17, 18, 22, 23, 26, 27, 30, 36, 37, 39, 41, 44, 45, 47, 49, 51, 57, 58, 59, 60, 62, 67, 68, 71, 73, 75, 76, 83, 88, 90, 91, 94, 95, 98, 101, 103, 105, 106, 112, 114, 117, 118, 125, 126, 128, 131, 133, 136
                
            Console.WriteLine("Grid1:" + TestUtils.GetPrimaryKeys(grid1, cache.Name)
                                  .Select(x => x.ToString()).Take(50).Aggregate((a, b) => a + ", " + b));
            
            Console.WriteLine("Grid2:" + TestUtils.GetPrimaryKeys(grid2, cache.Name)
                                  .Select(x => x.ToString()).Take(50).Aggregate((a, b) => a + ", " + b));
            
            Console.WriteLine("Grid3:" + TestUtils.GetPrimaryKeys(grid3, cache.Name)
                                  .Select(x => x.ToString()).Take(50).Aggregate((a, b) => a + ", " + b));
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