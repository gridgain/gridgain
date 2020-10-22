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

package org.apache.ignite.yardstick.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

/**
 * Ignite benchmark that performs transactional put operations with several operations per transaction.
 */
public class IgnitePutTxBatchBenchmark extends IgnitePutTxBenchmark {

    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        Set<Integer> keys = new TreeSet<>();

        while (keys.size() < args.batch())
            keys.add(nextRandom(args.range()));

        clo = new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteCache<Integer, Object> cache = cacheForOperation();

                for (Integer key : keys)
                    cache.put(key, new SampleValue(key));

                return null;
            }
        };
    }
}
