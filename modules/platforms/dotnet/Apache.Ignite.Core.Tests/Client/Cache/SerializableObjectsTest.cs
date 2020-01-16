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
    using Apache.Ignite.Core.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="ISerializable"/> object handling in Thin Client.
    /// </summary>
    public class SerializableObjectsTest : ClientTestBase
    {
        /** */
        private const int EntryCount = 50;
        
        // TODO: Compat tests on metadata handling
        // TODO: Serializable test with dynamic field set
        // TODO: Add GetAll test

        /// <summary>
        /// Tests DateTime metadata caching.
        /// </summary>
        [Test]
        public void TestDateTimeMetaCachingOnPut()
        {
            var cache = GetPopulatedCache();
            var res = cache.Query(new ScanQuery<int, DateTimeTest>()).GetAll();
            var requests = GetAllServerRequestNames().ToArray();

            // Verify that only one request is sent to the server:
            // metadata is already cached and should not be requested.
            Assert.AreEqual(new[] {"ClientCacheScanQuery"}, requests);
            
            // Verify results.
            Assert.AreEqual(EntryCount, res.Count);
            Assert.AreEqual(DateTimeTest.DefaultDateTime, res.Min(x => x.Value.Date));
        }

        /// <summary>
        /// Tests DateTime metadata caching.
        /// </summary>
        [Test]
        public void TestDateTimeMetaCachingOnGet()
        {
            // Retrieve data from a different client which does not yet have cached meta.
            var cache = GetClient().GetCache<int, DateTimeTest>(GetPopulatedCache().Name);
            var res = cache.Query(new ScanQuery<int, DateTimeTest>()).GetAll();
            var requests = GetAllServerRequestNames().ToArray();

            // Verify that only one BinaryTypeGet request per type is sent to the server.
            var expectedRequests = new[]
            {
                "ClientCacheScanQuery", 
                "ClientBinaryTypeNameGet",
                "ClientBinaryTypeGet",
                "ClientBinaryTypeNameGet",
                "ClientBinaryTypeGet"
            };
            Assert.AreEqual(expectedRequests, requests);
            
            // Verify results.
            Assert.AreEqual(EntryCount, res.Count);
            Assert.AreEqual(DateTimeTest.DefaultDateTime, res.Min(x => x.Value.Date));
        }
        
        /// <summary>
        /// Gets the populated cache.
        /// </summary>
        private ICacheClient<int, DateTimeTest> GetPopulatedCache()
        {
            var cacheName = TestContext.CurrentContext.Test.Name;
            var cache = Client.GetOrCreateCache<int, DateTimeTest>(cacheName);
            cache.PutAll(GetData(EntryCount).Select(x => new KeyValuePair<int, DateTimeTest>(x.Id, x)));

            ClearLoggers();
            return cache;
        }

        /// <summary>
        /// Gets the data.
        /// </summary>
        private static IEnumerable<DateTimeTest> GetData(int entryCount)
        {
            return Enumerable.Range(0, entryCount)
                .Select(x => new DateTimeTest
                {
                    Id = x,
                    Date = DateTimeTest.DefaultDateTime.AddHours(x),
                });
        }

        /// <summary>
        /// Test class with DateTime field.
        /// </summary>
        private class DateTimeTest
        {
            public static readonly DateTime DefaultDateTime = new DateTime(2002, 2, 2);
            public int Id { get; set; }
            public DateTime Date { get; set; }
        }
    }
}