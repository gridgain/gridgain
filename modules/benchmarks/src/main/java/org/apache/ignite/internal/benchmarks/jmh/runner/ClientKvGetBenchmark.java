package org.apache.ignite.internal.benchmarks.jmh.runner;

import org.apache.ignite.binary.BinaryObject;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.RunnerException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Fork(0)
public class ClientKvGetBenchmark extends ClientKvBenchmark {
    @Param("1000")
    private int keysPerThread;

    @Param("1000")
    private int loadBatchSize;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        AtomicInteger locCounter = new AtomicInteger();
        ThreadLocal<Integer> locBase = ThreadLocal.withInitial(() -> offset + locCounter.getAndIncrement() * 20_000_000);
        ThreadLocal<Long> locGen = ThreadLocal.withInitial(() -> 0L);

        Thread[] exe = new Thread[threads];

        System.out.println("Start loading: threads=" + threads);

        for (int i = 0; i < exe.length; i++) {
            exe[i] = new Thread(() -> {
                Map<Integer, BinaryObject> batch = new HashMap<>(loadBatchSize);

                for (int i1 = 0; i1 < keysPerThread; i1++) {
                    int key = nextId(locBase, locGen);
                    batch.put(key, tuple);

                    if (batch.size() == loadBatchSize) {
                        kvView.putAll(batch);
                        batch.clear();
                    }
                }
            });
        }

        for (Thread thread : exe) {
            thread.start();
        }

        for (Thread thread : exe) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println("Finish loading");
    }

    private final AtomicInteger counter = new AtomicInteger();

    private final ThreadLocal<Integer> base = ThreadLocal.withInitial(() -> offset + counter.getAndIncrement() * 20_000_000);

    private final ThreadLocal<long[]> gen = ThreadLocal.withInitial(() -> new long[1]);

    /**
     * Benchmark for KV upsert via embedded client.
     */
    @Benchmark
    public void get() {
        long[] cur = gen.get();
        cur[0] = cur[0] + 1;

        int id = (int) (base.get() + cur[0] % keysPerThread);

        BinaryObject val = kvView.get(id);
        assert val != null;
    }

    /**
     * Benchmark's entry point. Can be started from command line:
     *
     * ./gradlew ":runClientGetBenchmark" --args='jmh.keysPerThread=100000 jmh.loadBatchSize=1000 jmh.threads=4'
     */
    public static void main(String[] args) throws RunnerException {
        runBenchmark(ClientKvGetBenchmark.class, args);
    }
}
