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

namespace Apache.Ignite.BenchmarkDotNet.ThinClient
{
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client.Cache;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Scan query cursor GetAll benchmarks.
    /// <para />
    /// Compares the synchronous <c>GetAll</c> against the asynchronous <c>GetAllAsync</c> on a thin-client
    /// scan query cursor. The page size is small relative to the dataset, so every scan spans multiple server
    /// pages and the per-page fetch cost (where the sync and async paths diverge) dominates the measurement.
    /// </summary>
    [MemoryDiagnoser]
    public class ThinClientScanQueryGetAllBenchmark : ThinClientBenchmarkBase
    {
        /** */
        private const string CacheName = "scanCache";

        /** Number of entries to scan. */
        private const int EntryCount = 10000;

        /** Page size: small relative to EntryCount so the scan spans multiple server pages. */
        private const int PageSize = 512;

        /** */
        private ICacheClient<int, int> _cache;

        /** <inheritdoc /> */
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            _cache = Client.GetOrCreateCache<int, int>(CacheName);

            for (var i = 0; i < EntryCount; i++)
            {
                _cache[i] = i;
            }
        }

        /// <summary>
        /// Scan query GetAll (synchronous).
        /// </summary>
        [Benchmark(Baseline = true)]
        public void GetAll()
        {
            using var cursor = _cache.Query(GetScanQuery());
            cursor.GetAll();
        }

        /// <summary>
        /// Scan query GetAllAsync (asynchronous).
        /// </summary>
        [Benchmark]
        public void GetAllAsync()
        {
            using var cursor = _cache.Query(GetScanQuery());
            cursor.GetAllAsync().GetAwaiter().GetResult();
        }

        private static ScanQuery<int, int> GetScanQuery() => new()
        {
            PageSize = PageSize
        };
    }
}
