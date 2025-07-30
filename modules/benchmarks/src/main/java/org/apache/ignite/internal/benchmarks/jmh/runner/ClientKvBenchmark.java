/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.benchmarks.jmh.runner;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Benchmark for a single upsert operation via KV API with a possibility to disable updates via RAFT and to storage using a remote client.
 */
public class ClientKvBenchmark extends AbstractMultiNodeBenchmark {
    protected static final int DEFAULT_THREADS_COUNT = 4;

    protected IgniteCache<Integer, BinaryObject> kvView;

    @Param({"0"})
    protected int offset; // 1073741824 for second client to ensure unique keys

    @Param({"32"})
    private int partitionCount;

    private final AtomicInteger counter = new AtomicInteger();

    private final ThreadLocal<Integer> gen = ThreadLocal.withInitial(() -> offset + counter.getAndIncrement() * 20_000_000);

    protected BinaryObject tuple;

    /**
     * Initializes the tuple.
     */
    @Setup
    public void setUp() throws Exception {
        BinaryObjectBuilder builder = publicIgnite.binary().builder("tuple");
        for (int i = 1; i < 11; i++) {
            builder.setField("field" + i, FIELD_VAL);
        }
        tuple = builder.build();
        kvView = publicIgnite.cache(TABLE_NAME).withKeepBinary();
    }

    protected int nextId() {
        int cur = gen.get();
        gen.set(cur + 1);
        return cur;
    }

    protected int nextId(ThreadLocal<Integer> base, ThreadLocal<Long> gen) {
        long cur = gen.get();
        gen.set(cur + 1);
        return (int) (base.get() + cur);
    }

    @Override
    protected int nodes() {
        return 1;
    }

    @Override
    protected int partitionCount() {
        return partitionCount;
    }

    @Override
    protected int replicaCount() {
        return 1;
    }
}
