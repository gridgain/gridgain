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

package org.apache.ignite.internal.util.lang;

import com.sun.management.OperatingSystemMXBean;
import java.io.File;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import javax.management.MBeanServer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.java.JavaLogger;

public class IgniteMBeanUtils {
    private static final IgniteLogger LOG = new JavaLogger().getLogger(IgniteMBeanUtils.class);

    /** This is the name of the HotSpot Diagnostic MBean */
    private static final String HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";

    /** This is the type of the HotSpot Diagnostic MBean */
    private static final String HOTSPOT_BEAN_CLASS = "com.sun.management.HotSpotDiagnosticMXBean";

    /** Platform-specific management interface for the operating system. */
    private static final String OS_BEAN_NAME = "java.lang:type=OperatingSystem";

    /** HotSpot diagnostic MBean */
    private static Object hotspotMBean;

    /** Platform-specific operating system bean. */
    private static OperatingSystemMXBean osMBean;

    /** Buffer pool bean. */
    private static BufferPoolMXBean bufferPoolMXBean;

    static {
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool : pools) {
            if (pool.getName().equals("direct")) {
                bufferPoolMXBean = pool;
                break;
            }
        }

        try {
            hotspotMBean = getMBean(HOTSPOT_BEAN_NAME, Class.forName(HOTSPOT_BEAN_CLASS));
        }
        catch (Exception e) {
            // hotspot diagnostic bean is not supported.
            LOG.info("HotSpot diagnostic bean is not supported [err=" + e + ']');
        }

        try {
            osMBean = getMBean(OS_BEAN_NAME, OperatingSystemMXBean.class);
        }
        catch (Exception e) {
            // operating system bean is not supported.
            LOG.info("Operating system bean is not supported [err=" + e + ']');
        }
    }

    /**
     * Get MXBean from the platform MBeanServer.
     *
     * @param mxbeanName The name for uniquely identifying the MXBean within an MBeanServer.
     * @param mxbeanItf The MXBean interface.
     * @return A proxy for a platform MXBean interface.
     */
    private static <T> T getMBean(String mxbeanName, Class<T> mxbeanItf) {
        try {
            MBeanServer srv = ManagementFactory.getPlatformMBeanServer();

            return ManagementFactory.newPlatformMXBeanProxy(srv, mxbeanName, mxbeanItf);
        }
        catch (IOException | IllegalArgumentException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Returns a limit on the amount of memory that can be reserved for all Direct Byte Buffers.
     * If this limit is not explicitly set using JVM option {@code -XX:MaxDirectMemorySize}, for instance,
     * then this method returns {@link Runtime#maxMemory()} value. Note that is this case the real limit may depend on
     * JVM version and its internal implementation. For example, it might be limited to 87.5% of the maximum heap size
     * (Java 8), or limited to the maximum heap size (Java 11+).
     * Returns {@code -1} if HotSpot diagnostic bean is not supported.
     *
     * @return a limit on the amount of memory that can be reserved for all Direct Byte Buffers.
     * @throws IgniteException if getting a limit is failed for some reason.
     */
    public static long maxDirectMemorySize() throws IgniteException {
        if (hotspotMBean == null)
            return -1;

        try {
            Object vmOpt = hotspotMBean.getClass().getMethod("getVMOption", String.class)
                .invoke(hotspotMBean, "MaxDirectMemorySize");

            String val = (String) vmOpt.getClass().getMethod("getValue").invoke(vmOpt);

            long res = Long.parseLong(val);

            return res == 0 ? Runtime.getRuntime().maxMemory() : res;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Returns the estimated total capacity of all buffers in direct pool, measured in bytes.
     * This is the sum of the capacities of all buffer instances in the pool.
     * The capacity is the maximum amount of data a buffer can hold, regardless of how much is currently used.
     * Returns {@code -1} if the total capacity cannot be determined.
     *
     * @return Estimated total capacity of all buffers in direct pool, measured in bytes.
     */
    public static long directMemoryTotalCapacity() {
        if (bufferPoolMXBean == null)
            return -1;

        return bufferPoolMXBean.getTotalCapacity();
    }

    /**
     * Returns the estimated amount of memory (in bytes) that the JVM is using for direct buffer pool.
     * The returned value represents the actual native memory consumed outside the Java heap.
     * This value may differ from {@link #directMemoryTotalCapacity()} because it reflects the actual memory footprint,
     * which can include overhead.
     * Returns {@code -1} if the memory usage cannot be determined.
     *
     * @return Estimated amount of memory (in bytes) that the JVM is using for direct buffer pool.
     */
    public static long directMemoryUsed() {
        if (bufferPoolMXBean == null)
            return -1;

        return bufferPoolMXBean.getMemoryUsed();
    }

    /**
     * @return Committed VM size in bits.
     */
    public static long getCommittedVirtualMemorySize() {
        if (osMBean == null)
            return -1;

        return osMBean.getCommittedVirtualMemorySize();
    }

    /**
     * Call this method from your application whenever you
     * want to dump the heap snapshot into a file.
     *
     * @param fileName name of the heap dump file
     * @param live flag that tells whether to dump
     * only the live objects
     */
    public static void dumpHeap(String fileName, boolean live) {
        // initialize hotspot diagnostic MBean
        if (hotspotMBean == null)
            return;

        File f = new File(fileName);

        if (f.exists())
            f.delete();

        try {
            hotspotMBean.getClass().getMethod("dumpHeap", String.class, boolean.class)
                .invoke(hotspotMBean, fileName, live);
        }
        catch (RuntimeException re) {
            throw re;
        }
        catch (Exception exp) {
            throw new RuntimeException(exp);
        }
    }
}
