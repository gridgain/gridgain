﻿/*
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


namespace Apache.Ignite.Core.Tests.Cache.Query.Continuous
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using NUnit.Framework;

    /// <summary>
    /// Tests continuous queries.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class ContinuousQueryTest
    {
        /// <summary>
        /// Tests same query on multiple nodes.
        /// This tests verifies that there are no exception on Java side during event delivery.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestSameQueryMultipleNodes()
        {
            using (var ignite = StartIgnite())
            {
                var cache = ignite.GetOrCreateCache<Guid, Data>("data");
                cache.QueryContinuous(new ContinuousQuery<Guid, Data>(new Listener()));

                using (var ignite2 = StartIgnite())
                {
                    var cache2 = ignite2.GetOrCreateCache<Guid, Data>("data");
                    cache2.QueryContinuous(new ContinuousQuery<Guid, Data>(new Listener()));

                    for (var i = 0; i < 100; i++)
                    {
                        PutEntry(cache2);
                        PutEntry(cache);
                    }
                }
            }
        }

        /// <summary>
        /// Puts the entry and verifies events.
        /// </summary>
        private static void PutEntry(ICache<Guid, Data> cache)
        {
            // Put new entry.
            var entry = new Data {Id = Guid.NewGuid()};
            cache.Put(entry.Id, entry);

            // Wait for events.
            TestUtils.WaitForTrueCondition(() => Listener.Events.Count == 2);

            ICacheEntryEvent<Guid, Data> e;

            // Two listeners  - two events.
            Assert.IsTrue(Listener.Events.TryPop(out e));
            Assert.AreEqual(entry.Id, e.Key);

            Assert.IsTrue(Listener.Events.TryPop(out e));
            Assert.AreEqual(entry.Id, e.Key);
        }

        /// <summary>
        /// Starts the ignite.
        /// </summary>
        private static IIgnite StartIgnite()
        {
            return Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new Core.Binary.BinaryConfiguration(typeof(Data)),
                AutoGenerateIgniteInstanceName = true
            });
        }

        private class Data
        {
            public Guid Id;
        }

        private class Listener : ICacheEntryEventListener<Guid, Data>
        {
            public static readonly ConcurrentStack<ICacheEntryEvent<Guid, Data>> Events
                = new ConcurrentStack<ICacheEntryEvent<Guid, Data>>();

            public void OnEvent(IEnumerable<ICacheEntryEvent<Guid, Data>> evts)
            {
                foreach (var e in evts)
                {
                    Events.Push(e);
                }
            }
        }
    }
}
