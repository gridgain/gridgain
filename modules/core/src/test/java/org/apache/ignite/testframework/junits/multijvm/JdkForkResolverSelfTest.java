/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.testframework.junits.multijvm;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;

/**
 * Unit test for {@link JdkForkResolver}: the released-version -> JDK map and the
 * resolve/validate fail modes. Runs on any driver JDK.
 */
public class JdkForkResolverSelfTest {
    /** Driver JDK major. */
    private static final int LOC_MAJOR = U.majorJavaVersion(System.getProperty("java.version"));

    /** Apache Ignite versions: JDK 8, JDK 11 from 2.17.0 on. */
    @Test
    public void testRequiredMajorApacheLine() {
        JdkForkResolver r = new JdkForkResolver();

        assertEquals(8, r.requiredJdkMajor("2.1.0"));
        assertEquals(8, r.requiredJdkMajor("2.14.0"));
        assertEquals(8, r.requiredJdkMajor("2.15.0"));
        assertEquals(11, r.requiredJdkMajor("2.17.0"));
        assertEquals(11, r.requiredJdkMajor("2.18.0"));
    }

    /** A fork requiring the driver's own major inherits it: no path, no property read. */
    @Test
    public void testResolveInheritsDriverJdk() {
        assertNull(JdkForkResolver.resolveRemoteJavaHomeForVer("2.15.0", LOC_MAJOR));
    }

    /** Only the driver's JDK and legacy JDK 8 are supported fork targets. */
    @Test
    public void testResolveRejectsUnsupportedMajor() {
        int unsupported = LOC_MAJOR == 11 ? 13 : 11;

        GridTestUtils.assertThrows(null,
            () -> JdkForkResolver.resolveRemoteJavaHomeForVer("9.9.9", unsupported),
            AssertionError.class, "are supported");
    }

    /** A required JDK 8 fork without {@code test.multijvm.java.home} fails, not skips. */
    @Test
    public void testResolveFailsWhenPropertyUnset() {
        assumeTrue("Driver is JDK 8 itself: a JDK 8 fork inherits it instead", LOC_MAJOR != 8);

        String old = System.clearProperty(JdkForkResolver.TEST_MULTIJVM_JAVA_HOME);

        try {
            GridTestUtils.assertThrows(null,
                () -> JdkForkResolver.resolveRemoteJavaHomeForVer("2.15.0", 8),
                AssertionError.class, "Set -D" + JdkForkResolver.TEST_MULTIJVM_JAVA_HOME);
        }
        finally {
            if (old != null)
                System.setProperty(JdkForkResolver.TEST_MULTIJVM_JAVA_HOME, old);
        }
    }

    /** A {@code null} java home means the fork inherits the driver's JDK. */
    @Test
    public void testValidateInheritedReturnsLocalMajor() {
        assertEquals(LOC_MAJOR, JdkForkResolver.validateRemoteJre(null, false));
        assertEquals(LOC_MAJOR, JdkForkResolver.validateRemoteJre(null, true));
    }
}
