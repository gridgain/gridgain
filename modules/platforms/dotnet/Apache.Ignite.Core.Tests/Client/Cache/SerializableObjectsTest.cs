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
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Cache.Query;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="ISerializable"/> object handling in Thin Client.
    /// </summary>
    public class SerializableObjectsTest : ClientTestBase
    {
        // TODO: Add some compat tests on metadata handling!
        // TODO: Add Serializable test where new field is being added dynamically
        // TODO: Add GetAll check
        // TODO: Measure perf?

        [Test]
        public void TestDateTimeMeta()
        {
            const int entryCount = 5;
            
            var data = GetData(entryCount);

            var cache = Client.GetOrCreateCache<int, DateTimeTest>("foo");
            cache.PutAll(data.Select(x => new KeyValuePair<int, DateTimeTest>(x.Id, x)));

            ClearLoggers();
            
            var res = cache.Query(new ScanQuery<int, DateTimeTest>()).GetAll();
            var requests = GetAllServerRequestNames().ToArray();

            // Verify that only one request is sent to the server:
            // metadata is already cached and should not be requested.
            Assert.AreEqual(new[] {"ClientCacheScanQuery"}, requests);
            
            // Verify results.
            Assert.AreEqual(entryCount, res.Count);
            Assert.AreEqual(DateTimeTest.DefaultDateTime, res[0].Value.Date);
        }

        private static IEnumerable<DateTimeTest> GetData(int entryCount)
        {
            return Enumerable.Range(1, entryCount)
                .Select(x => new DateTimeTest
                {
                    Id = x,
                    Date = DateTimeTest.DefaultDateTime.AddHours(x),
                });
        }

        private class DateTimeTest
        {
            public static readonly DateTime DefaultDateTime = new DateTime(2002, 2, 2);
            public int Id { get; set; }
            public DateTime Date { get; set; }
        }
    }
}