namespace Apache.Ignite.Benchmarks.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;

    /// <summary>
    /// Cache GetAll benchmark.
    /// </summary>
    internal sealed class ScanQueryBenchmark : PlatformBenchmarkBase
    {
        /** Cache name. */
        private const string CacheName = "cache";
        
        /** Native cache wrapper. */
        private ICache<int, Employee> _cache;

        /// <summary>
        /// Gets the cache.
        /// </summary>
        private ICache<int, Employee> Cache
        {
            get { return _cache; }
        }

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            base.OnStarted();

            _cache = Node.GetCache<int, Employee>(CacheName);

            for (int i = 0; i < Emps.Length; i++)
                _cache.Put(i, Emps[i]);
        }
        
        /** <inheritDoc /> */
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            // TODO: Near caches
            descs.Add(BenchmarkOperationDescriptor.Create("ScanQueryMatchNone", ScanQueryMatchNone, 1));
            descs.Add(BenchmarkOperationDescriptor.Create("ScanQueryMatchAll", ScanQueryMatchAll, 1));
        }

        /// <summary>
        /// Scan.
        /// </summary>
        private void ScanQueryMatchNone(BenchmarkState state)
        {
            var filter = new Filter {ShouldMatch = false};
            Cache.Query(new ScanQuery<int, Employee>(filter)).GetAll();
        }

        /// <summary>
        /// Scan.
        /// </summary>
        private void ScanQueryMatchAll(BenchmarkState state)
        {
            var filter = new Filter {ShouldMatch = true};
            Cache.Query(new ScanQuery<int, Employee>(filter)).GetAll();
        }

        /// <summary>
        /// Scan query filter.
        /// </summary>
        private class Filter : ICacheEntryFilter<int, Employee>
        {
            /// <summary>
            /// Gets or sets a value indicating whether this filter should match all entries or none.
            /// </summary>
            public bool ShouldMatch { get; set; }

            /** <inheritdoc /> */
            public bool Invoke(ICacheEntry<int, Employee> entry)
            {
                return entry.Value.Age > int.MinValue && ShouldMatch;
            }
        }
    }
}