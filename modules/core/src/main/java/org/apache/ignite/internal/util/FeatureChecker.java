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
package org.apache.ignite.internal.util;

/**
 * Class extracted for fields from GridUnsafe to be absolutely independent with current and future static block
 * initialization effects.
 */
public class FeatureChecker {
    /**
     * Required JVM options for a node to run on JDK 17+: the canonical runtime list,
     * identical to {@code bin/include/jvmdefaults.sh} and {@code bin/include/jvmdefaults.bat}.
     * (The build/test list lives in {@code .mvn/jvm.config} and the parent poms' surefire
     * argLine and is a superset with test-only opens.)
     */
    private static final String JAVA_OPTIONS =
        "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED\n" +
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED\n" +
        "--add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED\n" +
        "--add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED\n" +
        "--add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED\n" +
        "--add-opens=java.base/jdk.internal.access=ALL-UNNAMED\n" +
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED\n" +
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\n" +
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED\n" +
        "--add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED\n" +
        "--add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED\n" +
        "--add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED\n" +
        "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.io=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.net=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.nio=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.security.cert=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.util=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.lang=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.math=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.time=ALL-UNNAMED\n" +
        "--add-opens=java.base/java.text=ALL-UNNAMED\n" +
        "--add-opens=java.base/sun.security.ssl=ALL-UNNAMED\n" +
        "--add-opens=java.base/sun.security.x509=ALL-UNNAMED\n" +
        "--add-opens=java.logging/java.util.logging=ALL-UNNAMED\n" +
        "--add-opens=java.management/sun.management=ALL-UNNAMED\n" +
        "--add-opens=java.desktop/java.awt.font=ALL-UNNAMED\n" +
        "--add-opens=java.sql/java.sql=ALL-UNNAMED";

    /** Warning appended to runtime errors caused by missing JVM options. */
    public static final String JAVA_VER_SPECIFIC_WARN =
        "\nPlease add the following parameters to JVM startup settings and restart the application: {parameters: " +
            JAVA_OPTIONS +
            "\n}" +
            "\nSee https://www.gridgain.com/docs/latest/getting-started/quick-start/java#running-gridgain-with-java-11-or-later "
            + "for more information.";
}
