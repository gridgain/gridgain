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
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.fail;

/**
 * Resolves and validates the JDK that must run a forked (remote) test node for a
 * previous-release version.
 *
 * <p>Holds the fork-JDK mechanism shared by the compatibility test frameworks (consumed
 * via the ignite-core test-jar) together with the released-version-to-JDK map. A subclass
 * overrides {@link #jdkByVer()} to supply a different release line's map, so the caller
 * picks the right resolver for its product line and no version-number parsing is needed here.
 *
 * <p>{@link #jdkByVer()} maps the first version of each new JDK tier to the required JDK
 * major; {@link #requiredJdkMajor(String)} floor-looks-up that map. To add a tier, append
 * one entry. A previous-release node forks under the JDK its line was certified on (JDK 8
 * before the JDK 17 transition); everything else inherits the driver's JDK.
 */
public class JdkForkResolver {
    /**
     * Property that specifies the JDK installation for legacy (JDK 8) remote forks.
     * TODO <a href="https://ggsystems.atlassian.net/browse/GG-49585">GG-49585</a>
     */
    public static final String TEST_MULTIJVM_JAVA_HOME = "test.multijvm.java.home";

    /** Apache Ignite (2.x) version -> required fork JDK major. */
    private static final NavigableMap<IgniteProductVersion, Integer> APACHE_JDK_BY_VER = new TreeMap<>();

    static {
        // Ignite 2.x: JDK 8, JDK 11 from 2.17.0 on.
        APACHE_JDK_BY_VER.put(IgniteProductVersion.fromString("2.0.0"), 8);
        APACHE_JDK_BY_VER.put(IgniteProductVersion.fromString("2.17.0"), 11);
    }

    /** @return Version -> required fork JDK major map for this product line. */
    protected NavigableMap<IgniteProductVersion, Integer> jdkByVer() {
        return APACHE_JDK_BY_VER;
    }

    /**
     * Returns the JDK major version that must run the forked remote node for a compat
     * test exercising the given previous-release version.
     *
     * @param ver Previous-release version string (e.g., {@code "2.14.0"}).
     * @return Required JDK major version for that version's fork.
     */
    public int requiredJdkMajor(String ver) {
        Map.Entry<IgniteProductVersion, Integer> e = jdkByVer().floorEntry(IgniteProductVersion.fromString(ver));

        return e == null ? 8 : e.getValue();
    }

    /**
     * Resolves the JDK installation root that should run the forked remote node for the
     * given previous-release version, or {@code null} to inherit the driver's own JDK.
     *
     * @param ver Previous-release version string.
     * @return Path to a JDK installation root, or {@code null} to inherit the driver's JDK.
     */
    @Nullable public String resolveRemoteJavaHome(String ver) {
        return resolveRemoteJavaHomeForVer(ver, requiredJdkMajor(ver));
    }

    /**
     * Resolves the JDK installation root for an explicit required JDK major, or {@code null}
     * to inherit the driver's own JDK.
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
     * unset or points at a non-JDK-8 install. A JDK 11 fork requirement also fails fast
     * because the JDK 11 fork launcher is not yet wired.
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
        // JDK 11 is expressible in the breakpoint tables but its fork launcher is not yet wired.
        if (requiredMajor != 8) {
            fail("Compat test ver=" + ver + " requires JDK " + requiredMajor + " for the remote fork, " +
                "but only the current substrate (JDK " + locMajor + ", inherited) and the legacy " +
                "JDK 8 are supported. JDK 11 fork wiring is a follow-up. See modules/compatibility/README.txt.");
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
