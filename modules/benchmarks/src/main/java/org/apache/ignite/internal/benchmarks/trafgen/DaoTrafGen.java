/*
 * Copyright (c) 2025 Hewlett-Packard Enterprise Development LP.
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of
 * Hewlett-Packard Enterprise Development LP.
 */
package org.apache.ignite.internal.benchmarks.trafgen;

import org.apache.ignite.internal.benchmarks.trafgen.utils.Metric;
import org.apache.ignite.internal.benchmarks.trafgen.utils.ThrowingConsumer;
import org.apache.ignite.internal.util.IgniteUtils;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * This program runs customizable CRUD traffic on DAO.
 */
public final class DaoTrafGen
{
    private static final String TAG1 = "index1";
    private static final String TAG2 = "index2";

    private static final String TAG1_VALUE_PREFIX = TAG1 + "_" + String.join("", Collections.nCopies(25, "v")) + "_";
    private static final String TAG2_VALUE_PREFIX = TAG2 + "_" + String.join("", Collections.nCopies(25, "v")) + "_";

    private static final int DEF_PAYLOAD_SIZE = 1024;

    // Options
    private static String optRealm = "realm0";
    private static String optStorage = "space0";
    private static byte[] binaryPayload = {};
    private static int optKeyRange = 0;
    private static GG8Sample handler;
    private static long warmup = 0;

    private DaoTrafGen()
    {
        // No instance
    }

    private interface AbstractCRUD
    {
        /**
         * Do the operation.
         *
         * @param aKeyPrefix
         *            Key prefix
         * @param iteration
         *            Current iteration, from 0 to {@code total - 1} (or some special negative values)
         *
         * @throws Exception
         *             Unexpected error
         */
        void call(String aKeyPrefix, int iteration) throws Exception;
    }

    /**
     * The atomic operations.
     */
    private enum CRUD implements AbstractCRUD
    {
        /**
         * .
         */
        CREATE_OR_REPLACE(DaoTrafGen::doCreateOrReplace),
        /**
         * .
         */
        GET(DaoTrafGen::doRead),
        /**
         * .
         */
        REPLACE(DaoTrafGen::doReplace),
        /**
         * .
         */
        DELETE(DaoTrafGen::doDelete),

        /**
         * .
         */
        TOTAL(null),;

        private final ThrowingConsumer<String, Exception> operation;

        CRUD(ThrowingConsumer<String, Exception> anOperation)
        {
            operation = anOperation;
        }

        @Override
        public void call(String aKeyPrefix, int iteration) throws Exception
        {
            operation.accept(aKeyPrefix + iteration);
        }
    }

    private enum PredefinedCallFlow
    {
        /**
         * Call model 1.
         */
        MODEL1(CRUD.CREATE_OR_REPLACE, CRUD.GET, CRUD.REPLACE, CRUD.GET, CRUD.DELETE),

        ;

        final CallFlow callflow;

        PredefinedCallFlow(AbstractCRUD... someOperations)
        {
            callflow = new CallFlow(someOperations);
        }
    }

    private static class CallFlow
    {
        private final AbstractCRUD[] operations;
        private final Map<AbstractCRUD, Metric> counters;

        CallFlow(AbstractCRUD... someOperations)
        {
            operations = someOperations;
            counters = new LinkedHashMap<>();
            counters.computeIfAbsent(CRUD.TOTAL, o -> new Metric(o.toString()));
            for (AbstractCRUD op : someOperations)
            {
                counters.computeIfAbsent(op, o -> new Metric(o.toString(), "   "));
            }
        }

        void runOneIteration(String keyPrefix, int iteration) throws Exception
        {
            for (AbstractCRUD operation : operations)
            {
                long ts = Metric.now();

                operation.call(keyPrefix, iteration);
                counters.get(operation).push(ts);
                counters.get(CRUD.TOTAL).push(ts);
            }
        }

        Collection<Metric> metrics()
        {
            return counters.values();
        }
    }

    /**
     * A call context.
     */
    private static class CallCtx extends Thread
    {
        private final CallFlow callflow;
        private final int iterations;
        private final String keyPrefix;

        CallCtx(String aKeyPrefix, CallFlow aCallFlow, int numIterations)
        {
            callflow = aCallFlow;
            iterations = numIterations;
            keyPrefix = aKeyPrefix;
        }

        @Override
        public void run()
        {
            try
            {
                // Loop on the specified call flow
                for (int count = 0; count < iterations; count++)
                {
                    final int keySuffix = (optKeyRange > 0) ? count % optKeyRange : count;
                    callflow.runOneIteration(keyPrefix, keySuffix);
                }

            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private static Set<String> makeSet(String val) {
        HashSet<String> set = IgniteUtils.newHashSet(1);
        set.add(val);
        return set;
    }

    private static void doCreateOrReplace(String key)
    {
        Map<String, Set<String>> lvTags = new HashMap<>();
        lvTags.put(TAG1, makeSet(TAG1_VALUE_PREFIX + key));

        handler.createOrReplace(key, lvTags, binaryPayload);
    }

    private static void doReplace(String key)
    {
        Map<String, Set<String>> lvTags = new HashMap<>();
        lvTags.put(TAG2, makeSet(TAG2_VALUE_PREFIX + key));
        handler.replace(key, lvTags, binaryPayload);
    }

    private static void doRead(String key)
    {
        byte[] lvData = handler.get(key);
        assert (lvData != null);
        assert (lvData.length == binaryPayload.length);
    }

    private static void doDelete(String key)
    {
        handler.delete(key);
    }

    private static void disableLoggers()
    {
    }

    /**
     * The entry point.
     *
     * @param args
     *            arguments
     * @throws DAOException
     *             dao exception
     * @throws InterruptedException
     *             thread exception
     */
    public static void main(String[] args) throws InterruptedException
    {
        CallFlow callFlow = new CallFlow(CRUD.CREATE_OR_REPLACE);
        int iterations = 100_000;
        int parallelism = 32;
        int optPayloadSize = DEF_PAYLOAD_SIZE;

        for (int ii = 0; ii < args.length; ii++)
        {
            switch (args[ii])
            {
                case "--call-flow":
                    callFlow = parseCallFlow(args[++ii]);
                    break;
                case "--iterations":
                    iterations = Integer.parseInt(args[++ii]);
                    break;
                case "--key-range":
                    optKeyRange = Integer.parseInt(args[++ii]);
                    break;
                case "--payload-size":
                    optPayloadSize = Integer.parseInt(args[++ii]);
                    break;
                case "--parallelism":
                    parallelism = Integer.parseInt(args[++ii]);
                    break;
                case "--realm":
                    optRealm = args[++ii];
                    break;
                case "--storage":
                    optStorage = args[++ii];
                    break;
                case "--warmup":
                    warmup = Long.parseLong(args[++ii]);
                    if (warmup < 0)
                    {
                        throw new IllegalArgumentException("Warmup time must be non-negative");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Invalid argument: " + args[ii]);
            }
        }
        disableLoggers();

        // Build the payload
        binaryPayload = buildPayload(optPayloadSize);

        handler = new GG8Sample();
        handler.intitialize(optRealm, optStorage);

        long startTime = System.currentTimeMillis();

        final CallFlow cf = callFlow;
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(() -> {
            cf.metrics().forEach(Metric::print);

            if (warmup > 0 && System.currentTimeMillis() - startTime > warmup) {
                System.out.println("Warm Up period is over, starting measurement.");
                cf.metrics().forEach(Metric::clear);
                warmup = 0;
            }
        }, 5, 5, TimeUnit.SECONDS);

        System.out.printf("Parallelism=" + parallelism + ", callFlow=" + callFlow.operations.length + ", iters=" + iterations);

        // Run the call flow
        final Thread[] threads = createThreads(parallelism, callFlow, iterations);
        startAllThenJoin(threads);

        callFlow.metrics().forEach(Metric::stop);
        service.shutdown();
        if (warmup > 0) {
            System.out.println("Warm Up period was not over! Metrics may be inaccurate!");
        }
        System.out.printf("Storage: %s %s%n", optRealm, optStorage);
        callFlow.metrics().forEach(Metric::print);
    }

    private static CallFlow parseCallFlow(String anArg)
    {
        try
        {
            return PredefinedCallFlow.valueOf(anArg).callflow;
        } catch (IllegalArgumentException e)
        {
            AbstractCRUD[] ops = Stream.of(anArg.split(",")).map(DaoTrafGen::parseOp).toArray(AbstractCRUD[]::new);
            return new CallFlow(ops);
        }
    }

    private static AbstractCRUD parseOp(String anOp)
    {
        return Enum.valueOf(CRUD.class, anOp);
    }

    // Constructs a binary record
    private static byte[] buildPayload(int size)
    {
        final byte[] payload = new byte[size];
        for (int ii = 0; ii < size; ii++)
        {
            payload[ii] = (byte) ii;
        }
        return payload;
    }

    private static void startAllThenJoin(final Thread[] threads) throws InterruptedException
    {
        for (Thread thread : threads)
        {
            thread.start();
        }
        for (Thread thread : threads)
        {
            thread.join();
        }
    }

    private static String keyPrefixOfThread(int threadIndex)
    {
        return "key-" + threadIndex + "-";
    }

    private static Thread[] createThreads(int parallelism, CallFlow callFlow, int iterations)
    {
        final Thread[] threads = new Thread[parallelism];
        Arrays.setAll(threads, ii -> new CallCtx(keyPrefixOfThread(ii), callFlow, iterations));
        return threads;
    }
}
