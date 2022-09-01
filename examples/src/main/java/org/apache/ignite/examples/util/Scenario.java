package org.apache.ignite.examples.util;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.management.MBeanServer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheFutureImpl;
import org.apache.ignite.lang.IgniteFuture;

public abstract class Scenario {

    private static final int CHAIN_COUNT = 100;
    private static final int ENTRY_SIZE_MB = 100;
    private static final String HEAP_DUMP = "/tmp/";

    private static final MemoryMXBean MEM = ManagementFactory.getMemoryMXBean();
    private static final ExecutorService EXEC = Executors.newSingleThreadExecutor();

    protected IgniteCache<Integer, int[]> cache;

    public Scenario setCache(IgniteCache<Integer, int[]> cache) {
        this.cache = cache;
        return this;
    }

    public abstract IgniteFuture<?> run(IgniteFuture<int[]> future) throws IgniteCheckedException, NoSuchFieldException, IllegalAccessException;

    public abstract int[] getValue(IgniteInternalFuture<?> chainedFuture) throws IgniteCheckedException;

    public abstract int[] getValue(IgniteFuture<?> future) throws IgniteCheckedException;

    public static void run(Scenario scenario) throws IllegalAccessException, NoSuchFieldException, IgniteCheckedException, InterruptedException {
        try (Ignite ignite = Ignition.start(new IgniteConfiguration().setWorkDirectory("/tmp/ignite"))) {
            IgniteCache<Integer, int[]> cache = ignite.createCache("cache");

            cache.put(0, generateValue(0));

            printMemoryUsage("BEFORE");
            IgniteFuture<?> finalFuture = scenario
                    .setCache(cache)
                    .run(cache.getAsync(0));

            int[] result = scenario.getValue(finalFuture);
            printResult(result);

            //askToGc();
            //dumpHeap(scenario.getClass(), true);
            printMemoryUsage("AFTER");

            // But turns out this future holds a strong reference to every single intermediate result.
            // And we can even access them all via reflection
            printHeldFutureValues(finalFuture, scenario);

        } finally {
            EXEC.shutdown();
        }
    }

    /**
     * Every time we call chain() we produce new value object
     */
    static class DirectChain extends Scenario {

        public static void main(String[] args) throws IllegalAccessException, NoSuchFieldException, IgniteCheckedException, InterruptedException {
            Scenario.run(new DirectChain());
        }

        @Override
        public IgniteFuture<?> run(IgniteFuture<int[]> future) throws IgniteCheckedException, NoSuchFieldException, IllegalAccessException {
            for (int i = 0; i < CHAIN_COUNT; i++) {
                future = future.chainAsync((f) -> {
                    int[] v = f.get();
                    return generateNew(v); // array value is a new object
                }, EXEC);
            }

            return future;
        }

        @Override
        public int[] getValue(IgniteInternalFuture<?> chainedFuture) throws IgniteCheckedException {
            return (int[]) chainedFuture.get();
        }

        @Override
        public int[] getValue(IgniteFuture<?> future) throws IgniteCheckedException {
            return (int[]) future.get();
        }
    }

    /**
     * Every time we call chain() we wrap actual value to ValueHolder and use it as future result,
     * so all futures will point to same ValueHolder instance while obsolete wrapped value would be released for GC
     */
    static class HolderChain extends Scenario {

        public static void main(String[] args) throws IllegalAccessException, NoSuchFieldException, IgniteCheckedException, InterruptedException {
            Scenario.run(new HolderChain());
        }

        @Override
        public IgniteFuture<?> run(IgniteFuture<int[]> future) throws IgniteCheckedException, NoSuchFieldException, IllegalAccessException {
            IgniteFuture<ValueHolder<int[]>> wrappedFuture = future.chainAsync(f -> new ValueHolder<>(f.get()), EXEC);
            for (int i = 0; i < CHAIN_COUNT; i++) {
                wrappedFuture = wrappedFuture.chainAsync(f -> {
                    ValueHolder<int[]> v = f.get();
                    return v.modify(Scenario::generateNew);  // actual array value is a new object but future value still the same
                }, EXEC);
            }

            return wrappedFuture;
        }

        @Override
        public int[] getValue(IgniteInternalFuture<?> chainedFuture) throws IgniteCheckedException {
            return ((ValueHolder<int[]>) chainedFuture.get()).getValue();
        }

        @Override
        public int[] getValue(IgniteFuture<?> future) throws IgniteCheckedException {
            return ((ValueHolder<int[]>) future.get()).getValue();
        }
    }

    static void printHeldFutureValues(IgniteFuture<?> finalFuture, Scenario scenario) throws IgniteCheckedException, NoSuchFieldException, IllegalAccessException {
        IgniteCacheFutureImpl<?> r = (IgniteCacheFutureImpl<?>) finalFuture;
        IgniteInternalFuture<?> nested = internalValue(r.internalFuture(), scenario);
        for (int i = 0; i < CHAIN_COUNT - 1; i++) {
            nested = internalValue(nested, scenario);
        }
    }

    static IgniteInternalFuture<?> internalValue(IgniteInternalFuture<?> future, Scenario scenario) throws NoSuchFieldException, IllegalAccessException, IgniteCheckedException {
        Field fut = future.getClass().getDeclaredField("fut");
        fut.setAccessible(true);
        IgniteInternalFuture<?> nested = (IgniteInternalFuture<?>) fut.get(future);
        printResult(scenario.getValue(nested));
        return nested;
    }

    static int[] generateValue(int id) {
        int intArraySize = ENTRY_SIZE_MB * 256 * 1024;
        int[] value = new int[intArraySize];
        value[0] = intArraySize;
        value[1] = id;
        return value;
    }

    static int[] generateNew(int[] oldVal) {
        return generateValue(oldVal[1] + 1);
    }

    static void printResult(int[] result) {
        System.out.println("Result: id[" + result[1] + "] size[" + result[0] + "]");
    }

    static void printMemoryUsage(String stageLabel) {
        printMemoryRow(stageLabel, "init", "used", "committed", "max");
        printMemoryUsage("Heap", MEM.getHeapMemoryUsage());
        printMemoryUsage("NonHeap", MEM.getNonHeapMemoryUsage());
        System.out.println();
    }

    static void printMemoryUsage(String category, MemoryUsage memoryUsage) {
        printMemoryRow(
                category,
                toMBs(memoryUsage.getInit()),
                toMBs(memoryUsage.getUsed()),
                toMBs(memoryUsage.getCommitted()),
                toMBs(memoryUsage.getMax())
        );
    }

    static void printMemoryRow(String label, String init, String used, String committed, String max) {
        System.out.format("%15s %10s %10s %10s %10s\n", label, init, used, committed, max);
    }

    private static String toMBs(long bytes) {
        return String.valueOf(bytes / (1024 * 1024));
    }

    public static void askToGc() throws InterruptedException {
        System.gc();
        // Give some time to JVM to perform GC
        for (int i = 0; i < 5000; i++)
            TimeUnit.MILLISECONDS.sleep(1);
    }

    public static void dumpHeap(Class<? extends Scenario> clz, boolean live) {
        String filePath = HEAP_DUMP + clz.getSimpleName() + ".hprof";

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(
                    server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class
            );
            mxBean.dumpHeap(filePath, live);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Value wrapper class to be used inside future chaining. The main trick is that each time we chain future,
     * introducing some addition future value post-processing, instead of returning raw value we return same ValueHolder,
     * thus making all futures to have a link to same object instance. Meanwhile, there would be no references
     * to intermediate values, so they could be garbage collected.
     */
    private static class ValueHolder<T> {

        private T value;

        public ValueHolder(T value) {
            this.value = value;
        }

        public T getValue() {
            return value;
        }

        /**
         * While this method is useful for preserving typesafety, previous future would still hold a reference
         * to this instance preventing it from GC
         */
        public <R> ValueHolder<R> map(Function<T, R> mapper) {
            return new ValueHolder<>(mapper.apply(value));
        }

        /**
         * Substitute current value with result of modifier function thus releasing old value for GC
         */
        public ValueHolder<T> modify(Function<T, T> modifier) {
            value = modifier.apply(value);
            return this;
        }

    }

}
