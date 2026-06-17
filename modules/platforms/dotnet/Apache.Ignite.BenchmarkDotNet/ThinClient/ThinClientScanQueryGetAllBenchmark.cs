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
    /// |      Method |     Mean |     Error |    StdDev | Ratio | RatioSD |   Gen 0 |   Gen 1 |   Gen 2 | Allocated |
    /// |------------ |---------:|----------:|----------:|------:|--------:|--------:|--------:|--------:|----------:|
    /// |      GetAll | 4.762 ms | 0.1990 ms | 0.5867 ms |  1.00 |    0.00 | 46.8750 | 15.6250 |       - | 632.77 KB |
    /// | GetAllAsync | 5.162 ms | 0.3892 ms | 1.1228 ms |  1.11 |    0.31 | 78.1250 | 70.3125 | 39.0625 | 697.36 KB |.
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
