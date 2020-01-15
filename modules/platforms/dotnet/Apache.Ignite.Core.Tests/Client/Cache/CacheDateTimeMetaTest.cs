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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Query;
    using NUnit.Framework;

    /// <summary>
    /// TODO: Better name. Test other types too (nested types, serializable types, etc).
    /// </summary>
    public class CacheDateTimeMetaTest : ClientTestBase
    {
        [Test]
        public void TestDateTimeMeta()
        {
            const int entryCount = 5;
            
            var data = Enumerable.Range(1, entryCount)
                .Select(x => new Foo
                {
                    Id = x,
                    StartDate = DateTime.Now.AddHours(x),
                    EndDate = DateTime.Now.AddDays(x)
                });

            var cache = Client.GetOrCreateCache<int, Foo>("foo");
            cache.PutAll(data.Select(x => new KeyValuePair<int, Foo>(x.Id, x)));

            ClearLoggers();
            
            var res = cache.Query(new ScanQuery<int, Foo>()).GetAll();
            var requests = GetAllServerRequestNames().ToArray();

            // Verify that only one request is sent to the server:
            // metadata is already cached and should not be requested.
            Assert.AreEqual(new[] {"ClientCacheScanQuery"}, requests);
            Assert.AreEqual(entryCount, res.Count);

            // TODO: Add some compat tests on metadata handling!
        }

        private class Foo
        {
            public int Id { get; set; }
            
            public DateTime? StartDate { get; set; }
            
            public DateTime? EndDate { get; set; }
        }
    }
}