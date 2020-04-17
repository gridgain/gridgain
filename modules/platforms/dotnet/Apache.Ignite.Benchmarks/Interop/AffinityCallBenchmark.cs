namespace Apache.Ignite.Benchmarks.Interop
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// Benchmark that uses AffinityCall + Platform Cache + Partition Scan.
    /// This is one of the fastest ways to do map-reduce style computation from .NET.
    /// </summary>
    internal sealed class AffinityCallBenchmark : PlatformBenchmarkBase
    {
        /** Cache with platform cache enabled. */
        private ICache<int, Employee> _cache;
        
        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            base.OnStarted();

            _cache = Node.GetCache<int, Employee>("cachePlatform");

            for (var i = 0; i < Emps.Length; i++)
            {
                _cache.Put(i, Emps[i]);
            }
        }
        
        /** <inheritDoc /> */
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("AffinityCallBenchmark", _ => AffinityCall(), 1));
        }

        /// <summary>
        /// Run benchmark iteration.
        /// </summary>
        private void AffinityCall()
        {
            var compute = Node.GetCompute();
            
            var func = new CharCountFunc
            {
                CacheName = _cache.Name,
                Char = 'e',
            };

            var cacheNames = new[] {_cache.Name};

            var maxPart = Node.GetAffinity(_cache.Name).Partitions;
            
            for (int part = 0; part < maxPart; part++)
            {
                func.Partition = part;

                compute.AffinityCall(cacheNames, part, func);
            }
        }

        private class CharCountFunc : IComputeFunc<long>
        {
            public char Char { get; set; }
            
            public int Partition { get; set; }
            
            public string CacheName { get; set; }
            
            [InstanceResource]
            public IIgnite Grid { get; set; }

            public long Invoke()
            {
                var cache = Grid.GetCache<int, Employee>(CacheName);

                // Local scan query with partition is served from Platform Cache.
                var scanQuery = new ScanQuery<int, Employee> {Local = true, Partition = Partition};

                return cache.Query(scanQuery).Sum(e => e.Value.Name.Count(c => c == Char));
            }
        }
    }
}