namespace Apache.Ignite.Benchmarks.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// AffinityCall benchmark.
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

            var func = new TestComputeFunc {Partition = 1};

            compute.AffinityCall(new[] {_cache.Name}, func.Partition, func);
        }

        private class TestComputeFunc : IComputeFunc<int>
        {
            public int Partition { get; set; }
            
            public int Invoke()
            {
                return Partition;
            }
        }
    }
}