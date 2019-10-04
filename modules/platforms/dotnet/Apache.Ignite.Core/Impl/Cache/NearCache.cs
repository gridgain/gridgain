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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System.Collections.Concurrent;

    /// <summary>
    /// Ignite cache wrapper with caching of deserialized objects in a local hash map (Dictionary).
    /// </summary>
    internal class NearCache<TK, TV> : CacheImpl<TK, TV>
    {
        // TODO: Init capacity from settings
        // TODO: Eviction
        // TODO: Is it ok to use .NET-based comparison here, because it differs from Java-based comparison for keys?
        /** */
        private readonly ConcurrentDictionary<TK, TV> _map = new ConcurrentDictionary<TK, TV>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="flagSkipStore">Skip store flag.</param>
        /// <param name="flagKeepBinary">Keep binary flag.</param>
        /// <param name="flagNoRetries">No-retries mode flag.</param>
        /// <param name="flagPartitionRecover">Partition recover mode flag.</param>
        /// <param name="flagAllowAtomicOpsInTx">Allow atomic operations in transactions flag.</param>
        public NearCache(
            IPlatformTargetInternal target,
            bool flagSkipStore,
            bool flagKeepBinary,
            bool flagNoRetries,
            bool flagPartitionRecover,
            bool flagAllowAtomicOpsInTx)
            : base(target, flagSkipStore, flagKeepBinary, flagNoRetries, flagPartitionRecover, flagAllowAtomicOpsInTx)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override TV Get(TK key)
        {
            TV val;
            if (_map.TryGetValue(key, out val))
            {
                return val;
            }

            return base.Get(key);
        }

        /** <inheritdoc /> */
        public override void Put(TK key, TV val)
        {
            _map[key] = val;

            base.Put(key, val);
        }
    }
}
