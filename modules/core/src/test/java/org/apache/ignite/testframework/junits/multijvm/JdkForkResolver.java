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

import java.io.IOException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.fail;

/**
 * Resolves and validates the JDK that must run a forked (remote) test node.
 *
 * <p>Single home for the fork-JDK policy shared by the compatibility test frameworks
 * (consumed via the ignite-core test-jar): a previous-release node forks under the JDK
 * its release line was certified on (JDK 8 before the JDK 17 transition), everything
 * else inherits the driver's JDK.
 *
 * <p>Update {@link #JDK_17_REQUIRED_FROM} here - and only here - when the first JDK 17
 * release ships.
 */
public final class JdkForkResolver {
    /** Property that specifies the JDK installation for legacy (JDK 8) remote forks. */
    public static final String TEST_MULTIJVM_JAVA_HOME = "test.multijvm.java.home";

    /**
     * First release version requiring JDK 17 at runtime, inclusive. Versions strictly
     * older than this need a JDK 8 fork; versions at or after this need a JDK 17 fork.
     */
    public static final IgniteProductVersion JDK_17_REQUIRED_FROM = IgniteProductVersion.fromString("8.10.0");

    /** */
    private JdkForkResolver() {
        // No instances.
    }

    /**
     * Returns the JDK major version that must run the forked remote node for a compat
     * test exercising the given previous-release version.
     *
     * @param ver Previous-release version string (e.g., {@code "2.14.0"}, {@code "8.9.127"}).
     * @return JDK major version: 8 for versions older than {@link #JDK_17_REQUIRED_FROM}, else 17.
     */
    public static int requiredJdkMajor(String ver) {
        return IgniteProductVersion.fromString(ver).compareTo(JDK_17_REQUIRED_FROM) >= 0 ? 17 : 8;
    }

    /**
     * Resolves the JDK installation root that should run the forked remote node for the
     * given previous-release version, or {@code null} to inherit the driver's own JDK.
     *
     * @param ver Previous-release version string (for messaging).
     * @return Path to a JDK installation root, or {@code null} to inherit the driver's JDK.
     */
    @Nullable public static String resolveRemoteJavaHomeForVer(String ver) {
        return resolveRemoteJavaHomeForVer(ver, requiredJdkMajor(ver));
    }

    /**
     * Required-major-parameterized variant of {@link #resolveRemoteJavaHomeForVer(String)} for
     * unit testing.
     *
     * <p>Two cases only:
     *
     * <ul>
     *   <li>{@code requiredMajor} equals the driver's (current substrate) JDK major -
     *       inherit it; returns {@code null}, no property needed.</li>
     *   <li>{@code requiredMajor} is the legacy JDK 8 line - read
     *       {@code -Dtest.multijvm.java.home} (must point at a JDK 8 install).</li>
     * </ul>
     *
     * <p>Fails with an actionable message if a JDK 8 fork is required but the property is
     * unset or points at a non-JDK-8 install.
     *
     * @param ver Previous-release version string (for messaging).
     * @param requiredMajor JDK major version that the fork must use.
     * @return Path to a JDK installation root, or {@code null} to inherit the driver's JDK.
     */
    @Nullable static String resolveRemoteJavaHomeForVer(String ver, int requiredMajor) {
        int locMajor = U.majorJavaVersion(System.getProperty("java.version"));

        // A version certified for the current substrate JDK forks under the driver's own
        // JDK - inherit it (null), no property needed.
        if (requiredMajor == locMajor)
            return null;

        // The only other supported remote is the legacy JDK 8 line.
        if (requiredMajor != 8) {
            fail("Compat test ver=" + ver + " requires JDK " + requiredMajor + " for the remote fork, " +
                "but only the current substrate (JDK " + locMajor + ", inherited) and the legacy " +
                "JDK 8 are supported. See modules/compatibility/README.txt.");
        }

        String javaHome = System.getProperty(TEST_MULTIJVM_JAVA_HOME);

        if (javaHome == null) {
            fail("Compat test ver=" + ver + " requires a JDK 8 remote fork. Set " +
                "-D" + TEST_MULTIJVM_JAVA_HOME + "=<JDK 8 install path>. See modules/compatibility/README.txt.");
        }

        // Probe the configured path to catch the "property points at the wrong JDK" case.
        try {
            int actualMajor = new JavaVersionCommand().majorVersion(javaHome);

            if (actualMajor != 8) {
                fail("Compat test ver=" + ver + " requires a JDK 8 remote fork, but " +
                    "-D" + TEST_MULTIJVM_JAVA_HOME + "='" + javaHome + "' is a JDK " + actualMajor + " install.");
            }
        }
        catch (IOException | InterruptedException e) {
            fail("Compat test ver=" + ver + " failed to probe the configured " +
                "remote JDK at '" + javaHome + "': " + e + ". Verify the path points at a valid JDK install.");
        }

        return javaHome;
    }

    /**
     * Resolves and validates the remote fork's JDK major version.
     *
     * <p>The current substrate JDK (inherited when {@code javaHome} is {@code null}) is
     * always accepted. The legacy JDK 8 is accepted only when {@code acceptLegacyJdk8} is
     * {@code true} (compatibility tests); plain multi-JVM forks must run the driver's JDK
     * - the current build's class files aren't loadable by an older JVM. Anything else
     * fails fast.
     *
     * @param javaHome Resolved JDK install path, or {@code null} to inherit the driver's JDK.
     * @param acceptLegacyJdk8 Whether a JDK 8 remote is acceptable (compat tests only).
     * @return Major version of the remote JDK (equal to the local major when {@code javaHome}
     *     is {@code null}).
     */
    public static int validateRemoteJre(@Nullable String javaHome, boolean acceptLegacyJdk8) {
        int locMajor = U.majorJavaVersion(System.getProperty("java.version"));

        if (javaHome == null)
            return locMajor; // Inherited driver JDK - remote equals local.

        int rmtMajor = -1;

        try {
            rmtMajor = new JavaVersionCommand().majorVersion(javaHome);
        }
        catch (IOException | InterruptedException e) {
            fail("Failed to probe remote JDK at '" + javaHome + "': " + e);
        }

        if (rmtMajor != locMajor && !(acceptLegacyJdk8 && rmtMajor == 8)) {
            fail("Unsupported remote fork JDK: local JDK major=" + locMajor + ", remote JDK major=" +
                rmtMajor + " (configured via -D" + TEST_MULTIJVM_JAVA_HOME + "='" + javaHome + "'). " +
                (acceptLegacyJdk8
                    ? "Only the current substrate JDK or the legacy JDK 8 may run a forked node."
                    : "Plain multi-JVM forks must run the driver's JDK; only compatibility tests may " +
                        "fork the legacy JDK 8.") +
                " See modules/compatibility/README.txt.");
        }

        return rmtMajor;
    }
}
