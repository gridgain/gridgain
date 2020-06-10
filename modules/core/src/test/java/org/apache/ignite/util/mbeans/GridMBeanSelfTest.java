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

package org.apache.ignite.util.mbeans;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.StandardMBean;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.mxbean.IgniteStandardMXBean;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.mxbean.MXBeanParametersDescriptions;
import org.apache.ignite.mxbean.MXBeanParametersNames;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * MBean test.
 */
public class GridMBeanSelfTest extends GridCommonAbstractTest {
    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testCorrectMBeanInfo() throws Exception {
        StandardMBean mbean = new IgniteStandardMXBean(new GridMBeanImplementation(), GridMBeanInterface.class);

        MBeanInfo info = mbean.getMBeanInfo();

        assertEquals("MBeanDescription.", info.getDescription());

        assertEquals(2, info.getOperations().length);

        for (MBeanOperationInfo opInfo : info.getOperations()) {
            if (opInfo.getDescription().equals("MBeanOperation.")) {
                assertEquals(2, opInfo.getSignature().length);

                for (MBeanParameterInfo paramInfo : opInfo.getSignature()) {
                    if (paramInfo.getName().equals("ignored"))
                        assertTrue(paramInfo.getDescription().equals("MBeanOperationParameter1."));
                    else {
                        assertTrue(paramInfo.getName().equals("someData"));
                        assertTrue(paramInfo.getDescription().equals("MBeanOperationParameter2."));
                    }
                }
            }
            else {
                assertEquals("MBeanSuperOperation.", opInfo.getDescription());
                assertEquals(1, opInfo.getSignature().length);
            }
        }

        assertEquals("Expected 4 attributes but got " + info.getAttributes().length,
            4, info.getAttributes().length);

        for (MBeanAttributeInfo attrInfo : info.getAttributes()) {
            if (!attrInfo.isWritable()) {
                assertTrue(attrInfo.getDescription().equals("MBeanReadonlyGetter.") ||
                    attrInfo.getDescription().equals("MBeanROGetter."));
            }
            else {
                assertTrue(attrInfo.getDescription().equals("MBeanWritableGetter.") ||
                    attrInfo.getDescription().equals("MBeanWritableIsGetter."));
            }
        }
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testMissedNameMBeanInfo() throws Exception {
        try {
            StandardMBean mbean = new IgniteStandardMXBean(new GridMBeanImplementation(), GridMBeanInterfaceBad.class);

            mbean.getMBeanInfo();
        }
        catch (AssertionError ignored) {
            return;
        }

        fail();
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testMissedDescriptionMBeanInfo() throws Exception {
        try {
            StandardMBean mbean = new IgniteStandardMXBean(new GridMBeanImplementation(),
                GridMBeanInterfaceBadAgain.class);

            mbean.getMBeanInfo();
        }
        catch (AssertionError ignored) {
            return;
        }

        fail();
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testEmptyDescriptionMBeanInfo() throws Exception {
        try {
            StandardMBean mbean = new IgniteStandardMXBean(new GridMBeanImplementation(),
                GridMBeanInterfaceEmptyDescription.class);

            mbean.getMBeanInfo();
        }
        catch (AssertionError ignored) {
            return;
        }

        fail();
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testEmptyNameMBeanInfo() throws Exception {
        try {
            StandardMBean mbean = new IgniteStandardMXBean(new GridMBeanImplementation(),
                GridMBeanInterfaceEmptyName.class);

            mbean.getMBeanInfo();
        }
        catch (AssertionError ignored) {
            return;
        }

        fail();
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testIgniteKernalReturnsValidMBeanInfo() throws Exception {
        try {
            IgniteEx igniteCrd = startGrid(0);

            IgniteMXBean igniteMXBean = (IgniteMXBean)startGrid(1);

            assertNotNull(igniteMXBean.getUserAttributesFormatted());
            assertNotNull(igniteMXBean.getLifecycleBeansFormatted());

            String coordinatorFormatted = igniteMXBean.getCurrentCoordinatorFormatted();

            assertTrue(coordinatorFormatted.contains(igniteCrd.localNode().addresses().toString()));
            assertTrue(coordinatorFormatted.contains(igniteCrd.localNode().hostNames().toString()));
            assertTrue(coordinatorFormatted.contains(Long.toString(igniteCrd.localNode().order())));
            assertTrue(coordinatorFormatted.contains(igniteCrd.localNode().id().toString()));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tests correct MBean interface.
     *
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testIgniteKernalReturnsValidPublicThreadPoolSize() throws Exception {
        try {
            IgniteEx igniteCrd = startGrid(0);

            IgniteMXBean igniteMXBean = getMxBean(igniteCrd.name(), "Kernal", IgniteMXBean.class, IgniteKernal.class);

            assertEquals(IgniteConfiguration.DFLT_PUBLIC_THREAD_CNT, igniteMXBean.getPublicThreadPoolSize());
        }
        finally {
            stopGrid(0);
        }
    }

    /**
     * Super interface for {@link GridMBeanInterface}.
     */
    public static interface GridMBeanSuperInterface {
        /**
         * Test getter.
         *
         * @return Some string.
         */
        @MXBeanDescription("MBeanROGetter.")
        public String getROData();

        /**
         * Test MBean operation.
         *
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanDescription("MBeanSuperOperation.")
        @MXBeanParametersNames({"someData"})
        @MXBeanParametersDescriptions({"MBeanOperationParameter1."})
        public String doSomethingSuper(String someData);
    }

    /**
     * Test MBean interface.
     */
    @MXBeanDescription("MBeanDescription.")
    public static interface GridMBeanInterface extends GridMBeanSuperInterface {
        /**
         * Test getter.
         *
         * @return Some string.
         */
        @MXBeanDescription("MBeanWritableGetter.")
        public String getWritableData();

        /**
         * Test setter.
         *
         * @param data Some string.
         */
        public void setWritableData(String data);

        /**
         * Test getter.
         *
         * @return Some string.
         */
        @MXBeanDescription("MBeanReadonlyGetter.")
        public String getReadOnlyData();

        /**
         * Test boolean getter.
         *
         * @return Some string.
         */
        @MXBeanDescription("MBeanWritableIsGetter.")
        public boolean isWritable();

        /**
         * Test boolean setter.
         *
         * @param isWritable Just a boolean.
         */
        public void setWritable(boolean isWritable);

        /**
         * Test MBean operation.
         *
         * @param ignored Some value.
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanDescription("MBeanOperation.")
        @MXBeanParametersNames({"ignored", "someData"})
        @MXBeanParametersDescriptions({"MBeanOperationParameter1.", "MBeanOperationParameter2."})
        public String doSomething(boolean ignored, String someData);
    }

    /**
     * Test MBean interface.
     */
    public static interface GridMBeanInterfaceBad {
        /**
         * Test MBean operation.
         *
         * @param ignored Some value.
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanDescription("MBeanOperation.")
        @MXBeanParametersNames({"ignored"})
        @MXBeanParametersDescriptions({"MBeanOperationParameter1.", "MBeanOperationParameter2."})
        public String doSomethingBad(boolean ignored, String someData);
    }

    /**
     * Test MBean interface.
     */
    public static interface GridMBeanInterfaceEmptyDescription {
        /**
         * Test MBean operation.
         *
         * @param ignored Some value.
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanDescription("")
        public String doSomethingBad(boolean ignored, String someData);
    }

    /**
     * Test MBean interface.
     */
    public static interface GridMBeanInterfaceEmptyName {
        /**
         * Test MBean operation.
         *
         * @param ignored Some value.
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanParametersNames({"", "someData"})
        public String doSomethingBadAgain(boolean ignored, String someData);
    }

    /**
     * Test MBean interface.
     */
    public static interface GridMBeanInterfaceBadAgain {
        /**
         * Test MBean operation.
         *
         * @param ignored Some value.
         * @param someData Some data.
         * @return Some string.
         */
        @MXBeanDescription("MBeanOperation.")
        @MXBeanParametersNames({"ignored", "someData"})
        @MXBeanParametersDescriptions({"MBeanOperationParameter1."})
        public String doSomethingBadAgain(boolean ignored, String someData);
    }

    /**
     * Test MBean implementation.
     */
    public class GridMBeanImplementation implements GridMBeanInterface, GridMBeanInterfaceBad,
        GridMBeanInterfaceBadAgain, GridMBeanInterfaceEmptyDescription, GridMBeanInterfaceEmptyName {

        /** {@inheritDoc} */
        @Override public String doSomething(boolean ignored, String someData) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String getReadOnlyData() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String getWritableData() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void setWritableData(String data) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String doSomethingBad(boolean ignored, String someData) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String doSomethingBadAgain(boolean ignored, String someData) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isWritable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void setWritable(boolean isWritable) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String getROData() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String doSomethingSuper(String someData) {
            return null;
        }
    }
}
