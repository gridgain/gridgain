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
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Events;
    using NUnit.Framework;

    /// <summary>
    /// Tests Near Cache behavior when primary node leaves.
    /// </summary>
    public class CacheNearNodeLeaveTest : IEventListener<IEvent>
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
        /// Tests that near cache is cleared when primary node for a given key leaves and there are no backups.
        /// </summary>
        [Test]
        public void TestPrimaryNodeLeaveNoBackupClearsNearCache()
        {
            var grid1 = Ignition.Start(TestUtils.GetTestConfiguration());
            var grid2 = Ignition.Start(TestUtils.GetTestConfiguration(name: "node2"));

            var cacheConfiguration = new CacheConfiguration("c") {NearConfiguration = new NearCacheConfiguration()};
            var cache = grid1.CreateCache<int, Foo>(cacheConfiguration);
            
            var key = TestUtils.GetPrimaryKey(grid2, cache.Name);
            cache[key] = new Foo(key);

            Assert.AreSame(cache.Get(key), cache.Get(key), "key is in near cache on grid1");

            // TODO: Figure out a good way to get NodeLeft callback without subscribing.
            grid1.GetEvents().EnableLocal(EventType.NodeLeft);
            grid1.GetEvents().LocalListen(this, EventType.NodeLeft);
            grid2.Dispose();
            Assert.IsTrue(grid1.WaitTopology(1));

            // TODO: See GridNearCacheEntry.valid()
            // * callback when invalid entry is detected - but this won't happen unless we try to read it
            // * call Java before returning any near entry - wasteful
            // * callback on topology change: remove all near entries (simple; potentially breaks subscription)
            
            // TODO:
            // We care only for NodeLeft event - in this case all keys for that node must be removed from Near Cache.
            // - simply iterate over all NearCacheEntry instances, call isValid, pass invalid keys to .NET. Then pass value to .NET again when entry becomes valid (if it does). 
            // - OR See ICache.GetLostPartitions - can we determine exact keys, or should we just remove all?
            Assert.IsEmpty(cache.GetAll(new[] {key}), "key is removed from near cache");
            Assert.Throws<KeyNotFoundException>(() => cache.Get(key), "key is removed from near cache");
        }
        
        /// <summary>
        /// Tests that near cache is not cleared when primary node for a given key leaves but there are backups.
        /// </summary>
        [Test]
        public void TestPrimaryNodeLeaveWithBackupKeepsNearCache()
        {
            
        }

        public bool Invoke(IEvent evt)
        {
            return true;
        }
    }
}