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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.Statement;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheContinuousQueryAsyncFailoverAtomicSelfTest
    extends CacheContinuousQueryFailoverAbstractSelfTest {

    @Parameterized.Parameters
    public static List<Object[]> pars() {
        return IntStream.range(0, 100)
            .mapToObj(i -> new Object[0])
            .collect(Collectors.toList());
    }

    @Rule
    public TestRule filterByName = new TestRule() {
        @Override public Statement apply(Statement base, Description description) {
            if (description.getMethodName().contains("testFailoverStartStopBackup"))
                return base;

            return new Statement() {
                @Override public void evaluate() throws Throwable {
                    // do nothing
                }
            };
        }
    };

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected boolean asyncCallback() {
        return true;
    }
}
