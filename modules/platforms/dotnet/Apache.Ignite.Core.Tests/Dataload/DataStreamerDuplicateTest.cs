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
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Datastream;
    using NUnit.Framework;

    public class DataStreamerDuplicateTest : TestBase
    {
        [Test]
        public void TestDuplicateCounter()
        {
            var cache = Ignite.GetOrCreateCache<string, long>("c");

            using (var streamer = Ignite.GetDataStreamer<string, long>(cache.Name))
            {
                streamer.AllowOverwrite = true;
                streamer.Receiver = new StreamTransformer<string, long, object, object>(new CountingEntryProcessor());

                var words = Enumerable.Repeat("a", 3).Concat(Enumerable.Repeat("b", 2));
                foreach (var word in words)
                {
                    streamer.AddData(word, 1L);
                }
            }

            Assert.AreEqual(3, cache.Get("a"));
            Assert.AreEqual(2, cache.Get("b"));
        }

        class CountingEntryProcessor : ICacheEntryProcessor<string, long, object, object>
        {
            public object Process(IMutableCacheEntry<string, long> e, object arg)
            {
                e.Value++;

                return null;
            }
        }
    }
}
