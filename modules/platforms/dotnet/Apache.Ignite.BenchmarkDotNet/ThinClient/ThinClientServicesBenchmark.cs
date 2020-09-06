/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.BenchmarkDotNet.ThinClient
{
    using System;
    using Apache.Ignite.BenchmarkDotNet.ThinClient.Services;
    using Apache.Ignite.Core;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Thin client services benchmark.
    /// <para />
    /// .NET Core 3.1.7
    /// |         Method |      Mean |     Error |    StdDev |    Median |  Gen 0 | Gen 1 | Gen 2 | Allocated |
    /// |--------------- |----------:|----------:|----------:|----------:|-------:|------:|------:|----------:|
    /// |  IntMethodThin |  32.10 us |  0.640 us |  1.496 us |  31.86 us | 0.3052 |     - |     - |   1.85 KB |
    /// | IntMethodThick | 153.68 us | 32.444 us | 94.641 us | 117.63 us | 0.1221 |     - |     - |   1.47 KB |
    /// </summary>
    [MemoryDiagnoser]
    public class ThinClientServicesBenchmark : ThinClientBenchmarkBase
    {
        /** */
        private const string ServiceName = nameof(BenchService);

        /** */
        private IIgnite ThickClient { get; set; }

        /** */
        private IBenchService ThickService { get; set; }

        /** */
        private IBenchService ThinService { get; set; }

        /** <inheritdoc /> */
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            var services = Ignite.GetServices();
            services.DeployClusterSingleton(ServiceName, new BenchService());

            var clientCfg = new IgniteConfiguration(Utils.GetIgniteConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "Client"
            };

            ThickClient = Ignition.Start(clientCfg);

            ThickService = ThickClient.GetServices().GetServiceProxy<IBenchService>(ServiceName);
            ThinService = Client.GetServices().GetServiceProxy<IBenchService>(ServiceName);
        }

        /** <inheritdoc /> */
        public override void GlobalCleanup()
        {
            ThickClient.Dispose();
            base.GlobalCleanup();
        }

        /** */
        [Benchmark]
        public void IntMethodThin()
        {
            var res = ThinService.Add(2, 3);

            if (res != 5)
            {
                throw new Exception();
            }
        }

        /** */
        [Benchmark]
        public void IntMethodThick()
        {
            var res = ThickService.Add(2, 3);

            if (res != 5)
            {
                throw new Exception();
            }
        }
    }
}
