/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CheckpointerTest {

    @Test
    public void assertCheckpointWriteSpeedFormatting() {
        assertEquals("512", speedInKb(512 * 1024));
        assertEquals("64", speedInKb(64 * 1024));
        assertEquals("64", speedInKb(64 * 1024 + 64));  // 64.0625
        assertEquals("10", speedInKb(10 * 1024));
        assertEquals("9", speedInKb(9 * 1024));
        assertEquals("9.5", speedInKb(9 * 1024 + 512)); // 9.5
        assertEquals("9.06", speedInKb(9 * 1024 + 64)); // 9.0625
        assertEquals("1", speedInKb(1024));
        assertEquals("1.5", speedInKb(1024 + 512));     // 1.5
        assertEquals("1.06", speedInKb(1024 + 64));     // 1.0625
        assertEquals("0.88", speedInKb(896));           // 0.875
        assertEquals("0.0625", speedInKb(64));          // 0.0625
        assertEquals("0.0078", speedInKb(8));           // 0.0078125
    }

    /** Since {@link Checkpoint#pagesSize} and {@link DataStorageConfiguration#getPageSize()} are <code>ints</code>,
     * <code>cp.pagesSize * ds.pageSize</code> can produce Integer overflow thus leading to incorrect
     * CP write speed calculation
      */
    @Test
    public void assertNoOverflow() {
        int pages = Integer.MAX_VALUE / 1024;
        int pageSize = 4096;
        float duration = 5.05f;

        assertEquals("1622", Checkpointer.WriteSpeedFormatter.calculateAndFormatWriteSpeed(pages, pageSize, duration));
    }

    private static String speedInKb(long kbs) {
        return Checkpointer.WriteSpeedFormatter.formatWriteSpeed(U.KB * kbs);
    }
}
