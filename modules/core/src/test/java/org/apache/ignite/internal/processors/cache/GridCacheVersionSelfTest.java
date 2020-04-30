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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridCacheVersionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTopologyVersionDrId() throws Exception {
        GridCacheVersion ver = version(10, 0);

        assertEquals(10, ver.nodeOrder());
        assertEquals(0, ver.dataCenterId());

        // Check with max topology version and some dr IDs.
        ver = version(0x7FFFFFF, 0);
        assertEquals(0x7FFFFFF, ver.nodeOrder());
        assertEquals(0, ver.dataCenterId());

        ver = version(0x7FFFFFF, 15);
        assertEquals(0x7FFFFFF, ver.nodeOrder());
        assertEquals(15, ver.dataCenterId());

        ver = version(0x7FFFFFF, 31);
        assertEquals(0x7FFFFFF, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        // Check max dr ID with some topology versions.
        ver = version(11, 31);
        assertEquals(11, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        ver = version(256, 31);
        assertEquals(256, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        ver = version(1025, 31);
        assertEquals(1025, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        // Check overflow exception.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return version(0x7FFFFFF + 1, 1);
            }
        }, IllegalArgumentException.class, null);
    }

    /**
     * Test versions marshalling.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMarshalling() throws Exception {
        GridCacheVersion ver = version(1, 1);
        GridCacheVersionEx verEx = new GridCacheVersionEx(2, 2, 0, -1, ver);

        Marshaller marsh = createStandaloneBinaryMarshaller();

        byte[] verBytes = marsh.marshal(ver);
        byte[] verExBytes = marsh.marshal(verEx);

        GridCacheVersion verNew = marsh.unmarshal(verBytes, Thread.currentThread().getContextClassLoader());
        GridCacheVersionEx verExNew = marsh.unmarshal(verExBytes, Thread.currentThread().getContextClassLoader());

        assert ver.equals(verNew);
        assert verEx.equals(verExNew);
    }

    /**
     * @param nodeOrder Node order.
     * @param drId Data center ID.
     * @return Cache version.
     */
    private GridCacheVersion version(int nodeOrder, int drId) {
        return new GridCacheVersion(0, 0L, nodeOrder, drId);
    }
}
