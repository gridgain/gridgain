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

package org.apache.ignite.yardstick.cache.dml;

import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.apache.ignite.yardstick.cache.model.Person8;

/**
 * Ignite benchmark that performs SQL MERGE operations for entity with indexed fields.
 */
public class IgniteSqlMergeIndexedValue8Benchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        Person8 p = new Person8(key);

        cache.query(new SqlFieldsQuery("merge into Person8(_key, val1, val2, val3, val4, val5, val6, val7, val8) " +
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .setArgs(key, p.val1(), p.val2(), p.val3(), p.val4(), p.val5(), p.val6(), p.val7(), p.val8()));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic-index");
    }
}
