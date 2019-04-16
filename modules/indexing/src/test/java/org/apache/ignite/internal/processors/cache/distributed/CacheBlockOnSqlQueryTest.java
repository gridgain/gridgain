/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Random;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class CacheBlockOnSqlQueryTest extends CacheBlockOnReadAbstractTest {
    /** {@inheritDoc} */
    @Override @NotNull protected CacheReadBackgroundOperation<?, ?> getReadOperation() {
        return new CacheReadBackgroundOperation<Integer, TestingEntity>() {
            /** Random. */
            private Random random = new Random();

            /** {@inheritDoc} */
            @Override protected CacheConfiguration<Integer, TestingEntity> createCacheConfiguration() {
                return super.createCacheConfiguration().setIndexedTypes(Integer.class, TestingEntity.class);
            }

            /** {@inheritDoc} */
            @Override protected Integer createKey(int idx) {
                return idx;
            }

            /** {@inheritDoc} */
            @Override protected TestingEntity createValue(int idx) {
                return new TestingEntity(idx, idx);
            }

            /** {@inheritDoc} */
            @Override public void doRead() {
                int idx = random.nextInt(entriesCount());

                cache().query(
                    new SqlQuery<>(TestingEntity.class, "val >= ? and val < ?")
                        .setArgs(idx, idx + 500)
                ).getAll();
            }
        };
    }

    /**
     *
     */
    public static class TestingEntity {
        /** Id. */
        @QuerySqlField(index = true)
        public Integer id;

        /** Value. */
        @QuerySqlField(index = true)
        public double val;

        /**
         * Default constructor.
         */
        public TestingEntity() {
        }

        /**
         * @param id Id.
         * @param val Value.
         */
        public TestingEntity(Integer id, double val) {
            this.id = id;
            this.val = val;
        }
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStartServerAtomicPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStartServerTransactionalPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopServerAtomicPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopServerTransactionalPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopBaselineAtomicPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopBaselineTransactionalPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStartClientAtomicPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStartClientTransactionalPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopClientAtomicPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopClientTransactionalPartitioned() {
        // No-op.
    }
}
