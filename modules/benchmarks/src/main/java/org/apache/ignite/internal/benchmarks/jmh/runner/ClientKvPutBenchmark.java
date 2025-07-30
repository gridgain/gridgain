package org.apache.ignite.internal.benchmarks.jmh.runner;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.RunnerException;

public class ClientKvPutBenchmark extends ClientKvBenchmark {
    @Benchmark
    public void upsert() {
        kvView.put(nextId(), tuple);
    }

    /**
     * Benchmark's entry point:
     *
     * ./gradlew ":runClientPutBenchmark" --args='jmh.batch=10 jmh.threads=1'
     */
    public static void main(String[] args) throws RunnerException {
        runBenchmark(ClientKvPutBenchmark.class, args);
    }
}
