/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

public class CacheIgniteOutOfMemoryExceptionOnTtlTest extends AbstractCacheIgniteOutOfMemoryExceptionTest {
    /**
     * Tests that an expired entry removing does not require loading the value of the entry.
     */
    @Test
    public void testEagerTtl() throws IgniteInterruptedCheckedException {
        CacheConfiguration<Integer, Object> ccfg = cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 5000)))
            .setEagerTtl(true);

        IgniteCache<Integer, Object> cache = grid(0).getOrCreateCache(ccfg);

        Runtime.getRuntime().gc();

        int blobSize = (int) (100 * U.MB);

        cache.put(1, new byte[blobSize]);

        // Let's occupy all free memory.
        ArrayList<Object> unused = new ArrayList<>();
        IgniteBiTuple<HotSpotDiagnosticMXBean, VMOption> mbean = disableHeapDumpOnOutOfMemoryError();
        try {
            while (true) {
                try {
                    unused.add(new byte[(int) (50 * U.MB)]);
                } catch (OutOfMemoryError e) {
                    // We don't have enough space to allocate a new continous block.
                    // Let's remove one blob in order to have enough memory to process the request
                    // and to remove entry whe it is expired.
                    unused.remove(unused.size() - 1);
                    unused.remove(unused.size() - 1);
                    unused.remove(unused.size() - 1);
                    break;
                }
            }
        }
        finally {
            restoreHeapDumpOnOutOfMemoryError(mbean);
        }

        assertEquals(1, cache.size());

        waitForCondition(() -> 0 == cache.size(), 10000);

        assertFalse(failure.get());
    }
}
