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
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests scan queries.
    /// </summary>
    public class ScanQueryTest : ClientTestBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ScanQueryTest"/> class.
        /// </summary>
        public ScanQueryTest() : base(2)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            var cfg = base.GetIgniteConfiguration();

            cfg.ClientConnectorConfiguration = new ClientConnectorConfiguration
            {
                MaxOpenCursorsPerConnection = 3
            };

            return cfg;
        }

        /// <summary>
        /// Tests scan query without filter.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "ReturnValueOfPureMethodIsNotUsed")]
        public void TestNoFilter()
        {
            var cache = GetPersonCache();

            Action<IEnumerable<ICacheEntry<int, Person>>> checkResults = e =>
            {
                Assert.AreEqual(cache.Select(x => x.Value.Name).OrderBy(x => x).ToArray(),
                    e.Select(x => x.Value.Name).OrderBy(x => x).ToArray());
            };

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);

                var query = new ScanQuery<int, Person>();

                // GetAll.
                var cursor = clientCache.Query(query);
                checkResults(cursor.GetAll());

                // Can't iterate or call GetAll again.
                Assert.Throws<InvalidOperationException>(() => cursor.ToArray());
                Assert.Throws<InvalidOperationException>(() => cursor.GetAll());

                // Iterator.
                using (cursor = clientCache.Query(query))
                {
                    checkResults(cursor.ToArray());

                    // Can't iterate or call GetAll again.
                    Assert.Throws<InvalidOperationException>(() => cursor.ToArray());
                    Assert.Throws<InvalidOperationException>(() => cursor.GetAll());
                }

                // Partial iterator.
                using (cursor = clientCache.Query(query))
                {
                    var item = cursor.First();
                    Assert.AreEqual(item.Key.ToString(), item.Value.Name);
                }

                // Local.
                query.Local = true;
                var localRes = clientCache.Query(query).ToList();
                Assert.Less(localRes.Count, cache.GetSize());
            }
        }

        /// <summary>
        /// Tests async scan query returns the same results as the sync one.
        /// </summary>
        [Test]
        public async Task TestQueryAsync()
        {
            var cache = GetPersonCache();

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);

                var cursor = await clientCache.QueryAsync(new ScanQuery<int, Person>());

                Assert.AreEqual(
                    cache.Select(x => x.Value.Name).OrderBy(x => x).ToArray(),
                    cursor.GetAll().Select(x => x.Value.Name).OrderBy(x => x).ToArray());
            }
        }

        /// <summary>
        /// Tests scan query with .NET filter.
        /// </summary>
        [Test]
        public void TestWithFilter()
        {
            GetPersonCache();

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);

                // One result.
                var single = clientCache.Query(new ScanQuery<int, Person>(new PersonKeyFilter(3))).Single();
                Assert.AreEqual(3, single.Key);

#if !NETCOREAPP   // Serializing delegates is not supported on this platform.
                // Multiple results.
                var res = clientCache.Query(new ScanQuery<int, Person>(new PersonFilter(x => x.Name.Length == 1)))
                    .ToList();
                Assert.AreEqual(9, res.Count);

                // No results.
                res = clientCache.Query(new ScanQuery<int, Person>(new PersonFilter(x => x == null))).ToList();
                Assert.AreEqual(0, res.Count);
#endif
            }
        }

        /// <summary>
        /// Tests scan query with .NET filter in binary mode.
        /// </summary>
        [Test]
        public void TestWithFilterBinary()
        {
            GetPersonCache();

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);
                var binCache = clientCache.WithKeepBinary<int, IBinaryObject>();

                // One result.
                var single = binCache.Query(new ScanQuery<int, IBinaryObject>(new PersonIdFilterBinary(8))).Single();
                Assert.AreEqual(8, single.Key);
            }
        }

#if !NETCOREAPP  // Serializing delegates and exceptions is not supported on this platform.
        /// <summary>
        /// Tests the exception in filter.
        /// </summary>
        [Test]
        public void TestExceptionInFilter()
        {
            GetPersonCache();

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);

                var qry = new ScanQuery<int, Person>(new PersonFilter(x =>
                {
                    throw new ArithmeticException("foo");
                }));

                var ex = Assert.Throws<IgniteClientException>(() => clientCache.Query(qry).GetAll());
                Assert.AreEqual("foo", ex.Message);
            }
        }
#endif

        /// <summary>
        /// Tests multiple cursors with the same client.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "GenericEnumeratorNotDisposed")]
        public void TestMultipleCursors()
        {
            var cache = GetPersonCache();

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);

                var qry = new ScanQuery<int, Person>();

                var cur1 = clientCache.Query(qry).GetEnumerator();
                var cur2 = clientCache.Query(qry).GetEnumerator();
                var cur3 = clientCache.Query(qry).GetEnumerator();

                // MaxCursors = 3
                var ex = Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));
                Assert.AreEqual("Too many open cursors", ex.Message.Substring(0, 21));
                Assert.AreEqual(ClientStatusCode.TooManyCursors, ex.StatusCode);

                var count = 0;

                while (cur1.MoveNext())
                {
                    count++;

                    Assert.IsTrue(cur2.MoveNext());
                    Assert.IsTrue(cur3.MoveNext());

                    Assert.IsNotNull(cur1.Current);
                    Assert.IsNotNull(cur2.Current);
                    Assert.IsNotNull(cur3.Current);

                    Assert.AreEqual(cur1.Current.Key, cur2.Current.Key);
                    Assert.AreEqual(cur1.Current.Key, cur3.Current.Key);
                }

                Assert.AreEqual(cache.GetSize(), count);

                // Old cursors were auto-closed on last page, we can open new cursors now.
                var c1 = clientCache.Query(qry);
                var c2 = clientCache.Query(qry);
                var c3 = clientCache.Query(qry);

                Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));

                // Close one of the cursors.
                c1.Dispose();
                c1 = clientCache.Query(qry);
                Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));

                // Close cursor via GetAll.
                c1.GetAll();
                c1 = clientCache.Query(qry);
                Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));

                c1.Dispose();
                c2.Dispose();
                c3.Dispose();
            }
        }

#if NETCOREAPP
        /// <summary>
        /// Tests that <c>await using</c> and <see cref="System.IAsyncDisposable.DisposeAsync"/> close the
        /// server-side cursor asynchronously, freeing up a cursor slot just like synchronous disposal.
        /// </summary>
        [Test]
        public async Task TestAwaitUsingClosesServerCursor()
        {
            GetPersonCache();

            using var client = GetClient();
            var clientCache = client.GetCache<int, Person>(CacheName);
            var qry = new ScanQuery<int, Person>();

            // MaxCursors = 3: open the maximum number of cursors.
            var c1 = clientCache.Query(qry);
            var c2 = clientCache.Query(qry);
            var c3 = clientCache.Query(qry);

            Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));

            // Async dispose frees a server-side cursor slot; awaiting guarantees the close completed.
            await c1.DisposeAsync();

            // A new cursor can be opened now, and exiting await using closes it again.
            await using (clientCache.Query(qry))
            {
                Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));
            }

            // Idempotent: a second async dispose is a no-op.
            await c1.DisposeAsync();

            c1 = clientCache.Query(qry);

            c1.Dispose();
            c2.Dispose();
            c3.Dispose();
        }

        /// <summary>
        /// Tests that <see cref="System.IAsyncDisposable.DisposeAsync"/> is a no-op for a cursor that has
        /// already been drained by <c>GetAll</c> (the server closes the cursor when the last page is read,
        /// so no redundant resource-close request is sent).
        /// </summary>
        [Test]
        public async Task TestDisposeAsyncAfterGetAllIsNoOp()
        {
            var cache = GetPersonCache();

            using var client = GetClient();
            var clientCache = client.GetCache<int, Person>(CacheName);
            var qry = new ScanQuery<int, Person>();

            var cursor = clientCache.Query(qry);

            // GetAll drains all pages; the server-side cursor is closed automatically on the last page.
            Assert.AreEqual(cache.GetSize(), cursor.GetAll().Count);

            // DisposeAsync must not send a resource-close for the already-closed cursor (would throw otherwise).
            await cursor.DisposeAsync();

            // The slot was never leaked: all cursors can still be opened.
            await using (clientCache.Query(qry))
            await using (clientCache.Query(qry))
            await using (clientCache.Query(qry))
            {
                Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));
            }
        }

        /// <summary>
        /// Tests that async and synchronous disposal can be mixed in any order on the same cursor:
        /// the server-side cursor is closed exactly once and the slot is freed.
        /// </summary>
        [Test]
        public async Task TestDisposeAsyncAndDisposeAreInterchangeable()
        {
            GetPersonCache();

            using var client = GetClient();
            var clientCache = client.GetCache<int, Person>(CacheName);
            var qry = new ScanQuery<int, Person>();

            // DisposeAsync first, then synchronous Dispose is a no-op.
            var cursor = clientCache.Query(qry);
            await cursor.DisposeAsync();
            cursor.Dispose();

            // Synchronous Dispose first, then DisposeAsync is a no-op.
            cursor = clientCache.Query(qry);
            cursor.Dispose();
            await cursor.DisposeAsync();

            // Both cursors were closed: the connection is back to zero open cursors.
            var c1 = clientCache.Query(qry);
            var c2 = clientCache.Query(qry);
            var c3 = clientCache.Query(qry);
            Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));

            c1.Dispose();
            c2.Dispose();
            c3.Dispose();
        }

        /// <summary>
        /// Tests that <c>await foreach</c> over a scan query cursor (<see cref="System.Collections.Generic.IAsyncEnumerable{T}"/>)
        /// returns all entries, fetching multiple pages asynchronously from the server.
        /// </summary>
        [Test]
        public async Task TestAwaitForeachReturnsAllEntries()
        {
            var cache = GetPersonCache();

            using var client = GetClient();
            var clientCache = client.GetCache<int, Person>(CacheName);

            // Small page size forces multiple async batch requests (GetBatchAsync) during enumeration.
            var qry = new ScanQuery<int, Person> { PageSize = 128 };

            var keys = new List<int>();

            await foreach (var entry in await clientCache.QueryAsync(qry))
            {
                Assert.AreEqual(entry.Key.ToString(), entry.Value.Name);
                keys.Add(entry.Key);
            }

            CollectionAssert.AreEquivalent(Enumerable.Range(1, cache.GetSize()), keys);
        }

        /// <summary>
        /// Tests that <see cref="System.Collections.Generic.IAsyncEnumerable{T}.GetAsyncEnumerator"/> enforces the
        /// same single-consumption rules as the synchronous enumerator: it can not be obtained after
        /// <c>GetAll</c> or after the cursor has already been (asynchronously) enumerated.
        /// </summary>
        [Test]
        public void TestGetAsyncEnumeratorAfterGetAllOrSecondCallThrows()
        {
            GetPersonCache();

            using var client = GetClient();
            var clientCache = client.GetCache<int, Person>(CacheName);

            // GetAsyncEnumerator after GetAll throws.
            var cursor = clientCache.Query(new ScanQuery<int, Person>());
            cursor.GetAll();
            Assert.Throws<InvalidOperationException>(() => cursor.GetAsyncEnumerator());

            // Second GetAsyncEnumerator on the same cursor throws.
            cursor = clientCache.Query(new ScanQuery<int, Person>());
            cursor.GetAsyncEnumerator();
            Assert.Throws<InvalidOperationException>(() => cursor.GetAsyncEnumerator());
        }

        /// <summary>
        /// Tests that <c>await foreach</c> over a scan query cursor stops with an
        /// <see cref="OperationCanceledException"/> when the enumeration token is cancelled mid-iteration.
        /// </summary>
        [Test]
        public void TestAsyncEnumerationHonorsCancellation()
        {
            GetPersonCache();

            using var client = GetClient();
            var clientCache = client.GetCache<int, Person>(CacheName);

            // Small page size so iteration spans multiple async batches.
            var qry = new ScanQuery<int, Person> { PageSize = 32 };
            var cts = new CancellationTokenSource();

            var count = 0;

            Assert.CatchAsync<OperationCanceledException>(async () =>
            {
                await foreach (var entry in (await clientCache.QueryAsync(qry)).WithCancellation(cts.Token))
                {
                    Assert.IsNotNull(entry);

                    if (++count == 10)
                    {
                        await cts.CancelAsync();
                    }
                }
            });

            // Enumeration stopped right after cancellation, well before draining the whole cache.
            Assert.AreEqual(10, count);
        }

        /// <summary>
        /// Tests that a client scan query cursor that was not enumerated can be disposed synchronously without
        /// throwing, and that <c>Dispose</c>/<c>DisposeAsync</c> are idempotent and can be mixed. Guards against
        /// the cursor's synchronization primitive being disposed while it is still considered in use.
        /// </summary>
        [Test]
        public async Task TestScanQueryCursorDisposeIsIdempotentWhenNotDrained()
        {
            GetPersonCache();

            using var client = GetClient();
            var clientCache = client.GetCache<int, Person>(CacheName);

            // Small page size leaves the server-side cursor open (more pages remain) so _hasNext stays true.
            var qry = new ScanQuery<int, Person> { PageSize = 32 };

            // Sync Dispose of a never-enumerated cursor must not throw, and must be idempotent.
            var cursor = clientCache.Query(qry);
            Assert.DoesNotThrow(() => cursor.Dispose());
            Assert.DoesNotThrow(() => cursor.Dispose());

            // DisposeAsync of a non-drained cursor, then a second DisposeAsync and a sync Dispose, must all be safe.
            cursor = clientCache.Query(qry);
            await cursor.DisposeAsync();
            await cursor.DisposeAsync();
            Assert.DoesNotThrow(() => cursor.Dispose());
        }
#endif

        /// <summary>
        /// Gets the string cache.
        /// </summary>
        private static ICache<int, Person> GetPersonCache()
        {
            var cache = GetCache<Person>();

            cache.RemoveAll();
            cache.PutAll(Enumerable.Range(1, 10000).ToDictionary(x => x, x => new Person
            {
                Id = x,
                Name = x.ToString()
            }));

            return cache;
        }

#if !NETCOREAPP  // Serializing delegates and exceptions is not supported on this platform.
        /// <summary>
        /// Person filter.
        /// </summary>
        private class PersonFilter : ICacheEntryFilter<int, Person>
        {
            /** Filter predicate. */
            private readonly Func<Person, bool> _filter;

            /// <summary>
            /// Initializes a new instance of the <see cref="PersonFilter"/> class.
            /// </summary>
            /// <param name="filter">The filter.</param>
            public PersonFilter(Func<Person, bool> filter)
            {
                _filter = filter;
            }

            /** <inheritdoc /> */
            public bool Invoke(ICacheEntry<int, Person> entry)
            {
                return _filter(entry.Value);
            }
        }
#endif

        /// <summary>
        /// Person filter.
        /// </summary>
        private class PersonKeyFilter : ICacheEntryFilter<int, Person>
        {
            /** Key. */
            private readonly int _key;

            /// <summary>
            /// Initializes a new instance of the <see cref="PersonKeyFilter"/> class.
            /// </summary>
            public PersonKeyFilter(int key)
            {
                _key = key;
            }

            /** <inheritdoc /> */
            public bool Invoke(ICacheEntry<int, Person> entry)
            {
                return entry.Key == _key;
            }
        }

        /// <summary>
        /// Person filter.
        /// </summary>
        private class PersonIdFilterBinary : ICacheEntryFilter<int, IBinaryObject>
        {
            /** Key. */
            private readonly int _id;

            /// <summary>
            /// Initializes a new instance of the <see cref="PersonIdFilterBinary"/> class.
            /// </summary>
            public PersonIdFilterBinary(int id)
            {
                _id = id;
            }

            /** <inheritdoc /> */
            public bool Invoke(ICacheEntry<int, IBinaryObject> entry)
            {
                return entry.Value.GetField<int>("Id") == _id;
            }
        }
    }
}
