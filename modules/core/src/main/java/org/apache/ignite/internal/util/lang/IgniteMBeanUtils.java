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
import java.lang.management.ManagementFactory;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ignite.IgniteException;

public class IgniteMBeanUtils {
    /** This is the name of the HotSpot Diagnostic MBean */
    private static final String HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";

    /** This is the type of the HotSpot Diagnostic MBean */
    private static final String HOTSPOT_BEAN_CLASS = "com.sun.management.HotSpotDiagnosticMXBean";

    /** Platform-specific management interface for the operating system. */
    private static final String OS_BEAN_NAME = "java.lang:type=OperatingSystem";

    private static final String DIRECT_BUFFER_BEAN_NAME = "java.nio:type=BufferPool,name=direct";

    /** field to store the hotspot diagnostic MBean */
    private static volatile Object hotspotMBean;

    /** Call to {@link #initOSMBean()} before accessing. */
    private static volatile OperatingSystemMXBean osMBean;

    /**
     * Initialize field to store OperatingSystem MXBean.
     */
    private static void initOSMBean() {
        if (osMBean == null) {
            synchronized (IgniteMBeanUtils.class) {
                if (osMBean == null)
                    osMBean = getMBean(OS_BEAN_NAME, OperatingSystemMXBean.class);
            }
        }
    }

    /**
     * Initialize the hotspot diagnostic MBean field.
     */
    private static void initHotspotMBean() {
        if (hotspotMBean == null) {
            synchronized (IgniteMBeanUtils.class) {
                if (hotspotMBean == null) {
                    try {
                        hotspotMBean = getMBean(HOTSPOT_BEAN_NAME, Class.forName(HOTSPOT_BEAN_CLASS));
                    }
                    catch (ClassNotFoundException e) {
                        throw new IgniteException(e);
                    }
                }
            }
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
     * Returns {@code true} if HotSpotDiagnosticBean is supported by this JVM, and {@code false} otherwise.
     *
     * @return {@code true} if HotSpotDiagnosticBean is supported by this JVM.
     */
    public static boolean isHotSpotDiagnosticBeanSupported() {
       try {
           initHotspotMBean();
       }
       catch (Exception e) {
           // No-op
       }

       return hotspotMBean != null;
    }

    /**
     * Returns a limit on the amount of memory that can be reserved for all Direct Byte Buffers.
     * If this limit is not explicitly set using JVM option {@code -XX:MaxDirectMemorySize}, for instance,
     * then this method returns {@link Runtime#maxMemory()} value. Note that is this case the real limit may depend on
     * JVM version and its internal implementation. For example, it might be limited to 87.5% of the maximum heap size
     * (Java 8), or limited to the maximum heap size (Java 11+).
     *
     * Returns {@code -1} if HotSpot diagnostic bean is not supported.
     *
     * @return a limit on the amount of memory that can be reserved for all Direct Byte Buffers.
     * @throws IgniteException if getting a limit is failed for some reason.
     */
    public static long maxDirectMemorySize() throws IgniteException {
        if (!isHotSpotDiagnosticBeanSupported())
            return -1;

        try {
            Object vmOpt = hotspotMBean.getClass().getMethod("getVMOption", String.class)
                .invoke(hotspotMBean, "MaxDirectMemorySize");

            String val = (String) vmOpt.getClass().getMethod("getValue").invoke(vmOpt);

            long res = Long.parseLong(val);

            return res == 0? Runtime.getRuntime().maxMemory() : res;
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
        if (!isHotSpotDiagnosticBeanSupported())
            return -1;

        try {
            ObjectName directMemoryPool = new ObjectName("java.nio:type=BufferPool,name=direct");
            MBeanServer srv = ManagementFactory.getPlatformMBeanServer();
            return (Long) srv.getAttribute(directMemoryPool, "TotalCapacity");
        }
        catch (AttributeNotFoundException | InstanceNotFoundException e) {
            return -1;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
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
        if (!isHotSpotDiagnosticBeanSupported())
            return -1;

        try {
            ObjectName directMemoryPool = new ObjectName("java.nio:type=BufferPool,name=direct");
            MBeanServer srv = ManagementFactory.getPlatformMBeanServer();
            return (Long) srv.getAttribute(directMemoryPool, "MemoryUsed");
        }
        catch (AttributeNotFoundException | InstanceNotFoundException e) {
            return -1;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @return Committed VM size in bits.
     */
    public static long getCommittedVirtualMemorySize() {
        initOSMBean();

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
        try {
            initHotspotMBean();
        }
        catch (IgniteException e) {
            // Hotspot diagnostic bean is not supported.
            return;
        }

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

    public static void main(String[] args) {
        System.out.println(">>>>> max: " + IgniteMBeanUtils.maxDirectMemorySize());
        System.out.println(">>>>> total: " + IgniteMBeanUtils.directMemoryTotalCapacity());
        System.out.println(">>>>> used: " + IgniteMBeanUtils.directMemoryUsed());
    }
}
