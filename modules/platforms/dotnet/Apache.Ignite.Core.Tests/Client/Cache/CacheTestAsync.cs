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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Async cache test.
    /// </summary>
    [TestFixture]
    public sealed class CacheTestAsync : CacheTest
    {
        /** <inheritdoc /> */
        protected override ICacheClient<TK, TV> GetClientCache<TK, TV>(string cacheName = CacheName)
        {
            return new CacheClientAsyncWrapper<TK, TV>(base.GetClientCache<TK, TV>(cacheName));
        }

        /// <summary>
        /// Tests that async continuations are executed on a ThreadPool thread, not on response handler thread.
        /// </summary>
        [Test]
        public void TestAsyncContinuationIsExecutedOnThreadPool()
        {
            var cache = base.GetClientCache<int>();
            var task = cache.PutAsync(1, 1).ContinueWith(_ =>
            {
                Thread.CurrentThread.Abort();
            }, TaskContinuationOptions.ExecuteSynchronously);

            Assert.Throws<AggregateException>(() => task.Wait());

            Assert.AreEqual(1, cache.GetAsync(1).Result);
        }
    }
}
