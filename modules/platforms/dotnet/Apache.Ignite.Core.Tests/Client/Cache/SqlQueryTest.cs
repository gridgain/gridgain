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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests SQL queries via thin client.
    /// </summary>
    public class SqlQueryTest : SqlQueryTestBase
    {
        /// <summary>
        /// Tests the SQL query.
        /// </summary>
        [Test]
        public void TestSqlQuery()
        {
#pragma warning disable 618
            var cache = GetClientCache<Person>();

            // All items.
            var qry = new SqlQuery(typeof(Person), "where 1 = 1");
            Assert.AreEqual(Count, cache.Query(qry).Count());

            // All items local.
            qry.Local = true;
            Assert.Greater(Count, cache.Query(qry).Count());

            // Filter.
            qry = new SqlQuery(typeof(Person), "where Name like '%7'");
            Assert.AreEqual(7, cache.Query(qry).Single().Key);

            // Args.
            qry = new SqlQuery(typeof(Person), "where Id = ?", 3);
            Assert.AreEqual(3, cache.Query(qry).Single().Value.Id);

            // DateTime.
            qry = new SqlQuery(typeof(Person), "where DateTime > ?", DateTime.UtcNow.AddDays(Count - 1));
            Assert.AreEqual(Count, cache.Query(qry).Single().Key);

            // Invalid args.
            qry.Sql = null;
            Assert.Throws<ArgumentNullException>(() => cache.Query(qry));

            qry.Sql = "abc";
            qry.QueryType = null;
            Assert.Throws<ArgumentNullException>(() => cache.Query(qry));
#pragma warning restore 618
        }

        /// <summary>
        /// Tests the SQL query with distributed joins.
        /// </summary>
        [Test]
        public void TestSqlQueryDistributedJoins()
        {
#pragma warning disable 618
            var cache = GetClientCache<Person>();

            // Non-distributed join returns incomplete results.
            var qry = new SqlQuery(typeof(Person),
                string.Format("from \"{0}\".Person, \"{1}\".Person as p2 where Person.Id = 11 - p2.Id",
                    CacheName, CacheName2));

            Assert.Greater(Count, cache.Query(qry).Count());

            // Distributed join fixes the problem.
            qry.EnableDistributedJoins = true;
            Assert.AreEqual(Count, cache.Query(qry).Count());
#pragma warning restore 618
        }

        /// <summary>
        /// Tests that the async fields query returns the same results as the sync one.
        /// </summary>
        [Test]
        public async Task TestFieldsQueryAsync()
        {
            var cache = GetClientCache<Person>();

            var cursor = await cache.QueryAsync(new SqlFieldsQuery("select Id from Person"));

            CollectionAssert.AreEquivalent(Enumerable.Range(1, Count), cursor.Select(x => (int) x[0]));
            Assert.AreEqual("ID", cursor.FieldNames.Single());
        }

        /// <summary>
        /// Tests the fields query.
        /// </summary>
        [Test]
        public void TestFieldsQuery()
        {
            var cache = GetClientCache<Person>();

            // All items.
            var qry = new SqlFieldsQuery("select Id from Person");
            var cursor = cache.Query(qry);
            CollectionAssert.AreEquivalent(Enumerable.Range(1, Count), cursor.Select(x => (int) x[0]));
            Assert.AreEqual("ID", cursor.FieldNames.Single());

            // All items local.
            qry.Local = true;
            Assert.Greater(Count, cache.Query(qry).Count());

            // Filter.
            qry = new SqlFieldsQuery("select Name from Person where Id = ?", 1)
            {
                Lazy = true,
                PageSize = 5,
            };
            Assert.AreEqual("Person 1", cache.Query(qry).Single().Single());

            // DateTime.
            qry = new SqlFieldsQuery("select Id, DateTime from Person where DateTime > ?", DateTime.UtcNow.AddDays(9));
            cursor = cache.Query(qry);
            Assert.AreEqual(new[] {"ID", "DATETIME" }, cursor.FieldNames);
            Assert.AreEqual(cache[Count].DateTime, cursor.Single().Last());

            // Invalid args.
            qry.Sql = null;
            Assert.Throws<ArgumentNullException>(() => cache.Query(qry));
        }

        /// <summary>
        /// Tests the SQL fields query with distributed joins.
        /// </summary>
        [Test]
        public void TestFieldsQueryDistributedJoins()
        {
            var cache = GetClientCache<Person>();

            // Non-distributed join returns incomplete results.
            var qry = new SqlFieldsQuery(string.Format(
                "select p2.Name from \"{0}\".Person, \"{1}\".Person as p2 where Person.Id = 11 - p2.Id",
                CacheName, CacheName2));

            Assert.Greater(Count, cache.Query(qry).Count());

            // Distributed join fixes the problem.
            qry.EnableDistributedJoins = true;
            Assert.AreEqual(Count, cache.Query(qry).Count());
        }

        /// <summary>
        /// Tests the fields query timeout.
        /// </summary>
        [Test]
        public void TestFieldsQueryTimeout()
        {
            var cache = GetClientCache<Person>();

            cache.PutAll(Enumerable.Range(1, 1000).ToDictionary(x => x, x => new Person(x)));

            var qry = new SqlFieldsQuery("select * from Person p0, Person p1, Person p2")
            {
                Timeout = TimeSpan.FromMilliseconds(1)
            };

            Assert.Throws<IgniteClientException>(() => cache.Query(qry).GetAll());
        }

        /// <summary>
        /// Tests the fields query on a missing cache.
        /// </summary>
        [Test]
        public void TestFieldsQueryMissingCache()
        {
            var cache = Client.GetCache<int, Person>("I do not exist");
            var qry = new SqlFieldsQuery("select name from person")
            {
                Schema = CacheName
            };

            // Schema is set => we still check for cache existence.
            var ex = Assert.Throws<IgniteClientException>(() => cache.Query(qry).GetAll());
            Assert.AreEqual("Cache doesn't exist: I do not exist", ex.Message);

            // Schema not set => also exception.
            qry.Schema = null;
            ex = Assert.Throws<IgniteClientException>(() => cache.Query(qry).GetAll());
            Assert.AreEqual("Cache doesn't exist: I do not exist", ex.Message);
        }

        /// <summary>
        /// Tests fields query with custom schema.
        /// </summary>
        [Test]
        public void TestFieldsQueryCustomSchema()
        {
            var cache1 = Client.GetCache<int, Person>(CacheName);
            var cache2 = Client.GetCache<int, Person>(CacheName2);

            cache1.RemoveAll();

            var qry = new SqlFieldsQuery("select name from person");

            // Schema not set: cache name is used.
            Assert.AreEqual(0, cache1.Query(qry).Count());
            Assert.AreEqual(Count, cache2.Query(qry).Count());

            // Schema set to first cache: no results both cases.
            qry.Schema = cache1.Name;
            Assert.AreEqual(0, cache1.Query(qry).Count());
            Assert.AreEqual(0, cache2.Query(qry).Count());

            // Schema set to second cache: full results both cases.
            qry.Schema = cache2.Name;
            Assert.AreEqual(Count, cache1.Query(qry).Count());
            Assert.AreEqual(Count, cache2.Query(qry).Count());
        }

        /// <summary>
        /// Tests the DML.
        /// </summary>
        [Test]
        public void TestDml()
        {
            var cache = GetClientCache<Person>();

            var qry = new SqlFieldsQuery("insert into Person (_key, id, name) values (?, ?, ?)", -10, 1, "baz");
            var res = cache.Query(qry).GetAll();

            Assert.AreEqual(1, res[0][0]);
            Assert.AreEqual("baz", cache[-10].Name);
        }

        /// <summary>
        /// Tests <see cref="SqlFieldsQuery.Partitions"/> argument propagation and validation.
        /// </summary>
        [Test]
        public void TestPartitionsValidation()
        {
            var cache = GetClientCache<Person>();
            var qry = new SqlFieldsQuery("SELECT * FROM Person") { Partitions = new int[0] };

            var ex = Assert.Throws<IgniteClientException>(() => cache.Query(qry).GetAll());
            StringAssert.EndsWith("Partitions must not be empty.", ex.Message);

            qry.Partitions = new[] {-1, -2};
            ex = Assert.Throws<IgniteClientException>(() => cache.Query(qry).GetAll());
            StringAssert.EndsWith("Illegal partition", ex.Message);
        }

        /// <summary>
        /// Tests <see cref="SqlFieldsQuery.UpdateBatchSize"/> argument propagation and validation.
        /// </summary>
        [Test]
        public void TestUpdateBatchSizeValidation()
        {
            var cache = GetClientCache<Person>();
            var qry = new SqlFieldsQuery("SELECT * FROM Person") { UpdateBatchSize = -1 };

            var ex = Assert.Throws<IgniteClientException>(() => cache.Query(qry).GetAll());
            StringAssert.EndsWith("updateBatchSize cannot be lower than 1", ex.Message);
        }

        /// <summary>
        /// Tests <see cref="SqlFieldsQuery.Label"/> property propagation to running queries system view.
        /// </summary>
        [Test]
        public void TestFieldsQueryLabel()
        {
            var cache = GetClientCache<Person>();

            const string systemViewSql = "SELECT SQL, LOCAL, LABEL FROM SYS.SQL_QUERIES";
            const string label = "test-label-0";

            var results = cache.Query(new SqlFieldsQuery(systemViewSql)
            {
                Label = label,
                Local = true
            }).GetAll();

            // Should see 1 running query (itself)
            Assert.AreEqual(1, results.Count);
            var res = results[0];

            Assert.AreEqual(systemViewSql, res[0]);
            Assert.IsTrue((bool)res[1]);
            Assert.AreEqual(label, (string)res[2]);
        }

#if NETCOREAPP
        /// <summary>
        /// Tests that a fields query cursor (<c>ClientFieldsQueryCursor</c>) can be disposed with
        /// <c>await using</c>, returning all rows and closing the server-side cursor asynchronously.
        /// </summary>
        [Test]
        public async Task TestAwaitUsingFieldsQueryCursor()
        {
            var cache = GetClientCache<Person>();

            var qry = new SqlFieldsQuery("select Id from Person");

            await using var cursor = cache.Query(qry);

            CollectionAssert.AreEquivalent(Enumerable.Range(1, Count), cursor.Select(x => (int)x[0]));
        }

        /// <summary>
        /// Tests that <see cref="System.IAsyncDisposable.DisposeAsync"/> closes a fields query cursor that is
        /// still open on the server (only partially enumerated), and is idempotent.
        /// </summary>
        [Test]
        public async Task TestDisposeAsyncClosesOpenFieldsQueryCursor()
        {
            var cache = GetClientCache<Person>();

            // Small page size keeps the server-side cursor open so DisposeAsync sends a real close request.
            var qry = new SqlFieldsQuery("select Id from Person") { PageSize = 1 };
            var cursor = cache.Query(qry);

            // Read a single row, leaving the server-side cursor open (more pages remain).
            using var enumerator = cursor.GetEnumerator();
            Assert.IsTrue(enumerator.MoveNext());

            // DisposeAsync closes the still-open server cursor without blocking; repeated dispose is a no-op.
            await cursor.DisposeAsync();
            await cursor.DisposeAsync();
            cursor.Dispose();

            // Cursor is disposed: further iteration throws.
            Assert.Throws<ObjectDisposedException>(() => enumerator.MoveNext());
        }

        /// <summary>
        /// Tests that <c>await foreach</c> over a fields query cursor (<c>ClientFieldsQueryCursor</c>) returns all
        /// rows, fetching multiple pages asynchronously from the server.
        /// </summary>
        [Test]
        public async Task TestAwaitForeachFieldsQueryReturnsAllRows()
        {
            var cache = GetClientCache<Person>();

            // Small page size forces multiple async page requests during enumeration.
            var qry = new SqlFieldsQuery("select Id from Person order by Id") { PageSize = 1 };

            var ids = new List<int>();

            await foreach (var row in await cache.QueryAsync(qry))
            {
                ids.Add((int)row[0]);
            }

            CollectionAssert.AreEqual(Enumerable.Range(1, Count), ids);
        }

        /// <summary>
        /// Tests that <c>GetAllAsync</c> on a fields query cursor (<c>ClientFieldsQueryCursor</c>) returns all
        /// rows, paging from the server asynchronously without blocking the calling thread.
        /// </summary>
        [Test]
        public async Task TestFieldsQueryGetAllAsyncReturnsAllRows()
        {
            var cache = GetClientCache<Person>();

            // Small page size forces multiple async page requests inside GetAllAsync.
            var cursor = await cache.QueryAsync(new SqlFieldsQuery("select Id from Person order by Id") { PageSize = 1 });

            var rows = await cursor.GetAllAsync();

            CollectionAssert.AreEqual(Enumerable.Range(1, Count), rows.Select(r => (int)r[0]));
        }
#endif
    }
}
