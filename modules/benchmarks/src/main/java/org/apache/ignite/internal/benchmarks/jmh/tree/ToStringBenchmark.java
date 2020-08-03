/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.benchmarks.jmh.tree;

import java.util.function.Supplier;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

/**
 */
public class ToStringBenchmark extends JmhAbstractBenchmark {
//    private static final CurrentTineProvider timeProvider = System::currentTimeMillis;

    private static final Supplier<Long> timeSupplier = System::currentTimeMillis;

//    @Benchmark
//    public void timeProvider(Blackhole bh) {
//        bh.consume(timeProvider.currentTime());
//    }

    @Benchmark
    public void timeSupplier(Blackhole bh) {
        bh.consume(timeSupplier.get());
    }

    /**
     * Run benchmark.
     *
     * @throws Exception If failed.
     */
    private static void run(int threads) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(threads)
            .warmupIterations(10)
            .measurementIterations(10)
            .benchmarks(ToStringBenchmark.class.getSimpleName())
            .jvmArguments("-Xms4g", "-Xmx4g")
            .run();
    }

    public static void main(String[] args) throws Exception {
        run(1);
    }
}
