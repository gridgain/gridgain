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

namespace Apache.Ignite.BenchmarkDotNet.ThinClient.Services
{
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Benchmark service.
    /// </summary>
    public class BenchService : IService, IBenchService
    {
        /** <inheritdoc /> */
        public int Add(int x, int y)
        {
            return x + y;
        }

        /** <inheritdoc /> */
        public void Init(IServiceContext context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Execute(IServiceContext context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Cancel(IServiceContext context)
        {
            // No-op.
        }
    }
}
