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

package org.apache.ignite.internal.product;

import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteVersionUtils.BUILD_TSTAMP;
import static org.apache.ignite.internal.IgniteVersionUtils.REV_HASH_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.junit.Assert.assertArrayEquals;

/**
 * Versions test.
 */
public class GridProductVersionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFromString() throws Exception {
        IgniteProductVersion ver = IgniteProductVersion.fromString("1.2.3");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(0, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());
        assertEquals("1.2.3#19700101-sha1:00000000", ver.toString());

        ver = IgniteProductVersion.fromString("1.2.3-0-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals(0, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3.b1-4-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("b1", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("2.7.2-p14-1597656965-14d6499e102005cb5308a834976f1dc751a2775f");

        assertEquals(2, ver.major());
        assertEquals(7, ver.minor());
        assertEquals(2, ver.maintenance());
        assertEquals("p14", ver.stage());
        assertEquals(1597656965L, ver.revisionTimestamp());
        assertArrayEquals(new byte[] {
            20, -42, 73, -98, 16, 32, 5, -53, 83, 8, -88, 52, -105, 111, 29, -57, 81, -94, 119, 95 },
            ver.revisionHash());
        assertEquals("2.7.2-p14#20200817-sha1:14d6499e", ver.toString());

        ver = IgniteProductVersion.fromString("1.2.3.final-4-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("final", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(0, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3-4");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[]{24, -27, -89, -20, -98, 50, 2, 18, 106, 105, -68, 35, 26, 107, -106, 91, -63, -41, 61, -18},
            ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3.b1-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("b1", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[]{24, -27, -89, -20, -98, 50, 2, 18, 106, 105, -68, 35, 26, 107, -106, 91, -63, -41, 61, -18},
            ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3-rc1-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("rc1", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[]{24, -27, -89, -20, -98, 50, 2, 18, 106, 105, -68, 35, 26, 107, -106, 91, -63, -41, 61, -18},
            ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3-SNAPSHOT-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("SNAPSHOT", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[]{24, -27, -89, -20, -98, 50, 2, 18, 106, 105, -68, 35, 26, 107, -106, 91, -63, -41, 61, -18},
            ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3.b1-SNAPSHOT-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("b1-SNAPSHOT", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[]{24, -27, -89, -20, -98, 50, 2, 18, 106, 105, -68, 35, 26, 107, -106, 91, -63, -41, 61, -18},
            ver.revisionHash());

        IgniteProductVersion.fromString(VER_STR + '-' + BUILD_TSTAMP + '-' + REV_HASH_STR);
    }
}
