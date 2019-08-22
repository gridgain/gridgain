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

namespace Apache.Ignite.Core.Tests.Dataload
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Datastream;
    using NUnit.Framework;

    public class DataStreamerDuplicateTest : TestBase
    {
        [Test]
        public void TestDuplicateCounter()
        {
            var cfg = new CacheConfiguration("wordCountCache");
            var stmCache = Ignite.GetOrCreateCache<string, long>(cfg);

            using (var streamer = Ignite.GetDataStreamer<string, long>(stmCache.Name))
            {
                streamer.AllowOverwrite = true;

                // Configure data transformation to count instances of the same word
                streamer.Receiver = new StreamTransformer<string, long, object, object>(new MyEntryProcessor());

                foreach (var word in GetWords())
                {
                    streamer.AddData(word, 1L);
                }

                streamer.Flush();
            }

            Assert.AreEqual(3, stmCache.Get("a"));
            Assert.AreEqual(2, stmCache.Get("b"));
        }

        private static IEnumerable<string> GetWords()
        {
            return Enumerable.Repeat("a", 3).Concat(Enumerable.Repeat("b", 2));
        }

        class MyEntryProcessor : ICacheEntryProcessor<string, long, object, object>
        {
            public object Process(IMutableCacheEntry<string, long> e, object arg)
            {
                var val = e.Value;
                e.Value = val == 0 ? 1L : val + 1;

                return null;
            }
        }
    }
}
