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

package org.apache.ignite.internal.binary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Compares binary objects of different versions.
 */
public class BinaryMarshallerMixedVersionTest extends GridCommonAbstractTest {
    /** Marshaller v1. */
    private BinaryMarshaller marsh1 = binaryMarshaller((byte)1);

    /** Marshaller v2. */
    private BinaryMarshaller marsh2 = binaryMarshaller((byte)2);

    /**
     * Default constructor.
     */
    public BinaryMarshallerMixedVersionTest() throws IgniteCheckedException {
    }

    /**
     * @param obj Test object.
     */
    private void verify(Object obj) throws IgniteCheckedException {
        BinaryObjectExImpl bo1 = marshal(obj, marsh1);
        BinaryObjectExImpl bo2 = marshal(obj, marsh2);

        Assert.assertNotEquals(bo1.version(), bo2.version());
        Assert.assertEquals(bo1, bo2);
    }

    /** */
    @Test
    public void testNull() throws IgniteCheckedException {
        verify(null);
    }

    /** */
    @Test
    public void testByte() throws IgniteCheckedException {
        verify((byte)100);
    }

    /** */
    @Test
    public void testShort() throws IgniteCheckedException {
        verify((short)100);
    }

    /** */
    @Test
    public void testInt() throws IgniteCheckedException {
        verify(100);
    }

    /** */
    @Test
    public void testLong() throws IgniteCheckedException {
        verify(100L);
    }

    /** */
    @Test
    public void testFloat() throws IgniteCheckedException {
        verify(100.001f);
    }

    /** */
    @Test
    public void testDouble() throws IgniteCheckedException {
        verify(100.001d);
    }

    /** */
    @Test
    public void testChar() throws IgniteCheckedException {
        verify((char)100);
    }

    /** */
    @Test
    public void testBoolean() throws IgniteCheckedException {
        verify(true);
    }

    /** */
    @Test
    public void testDecimal() throws IgniteCheckedException {
        verify(BigDecimal.ZERO);
        verify(BigDecimal.valueOf(Long.MAX_VALUE, 0));
        verify(BigDecimal.valueOf(Long.MIN_VALUE, 0));
        verify(BigDecimal.valueOf(Long.MAX_VALUE, 8));
        verify(BigDecimal.valueOf(Long.MIN_VALUE, 8));
        verify(new BigDecimal(new BigInteger("-79228162514264337593543950336")));
    }


    /** */
    @Test
    public void testNegativeScaleDecimal() throws IgniteCheckedException {
        verify(BigDecimal.valueOf(Long.MAX_VALUE, -1));
        verify(BigDecimal.valueOf(Long.MIN_VALUE, -2));
        verify(BigDecimal.valueOf(Long.MAX_VALUE, -3));
        verify(BigDecimal.valueOf(Long.MIN_VALUE, -4));
    }

    /** */
    @Test
    public void testNegativeScaleRoundingModeDecimal() throws IgniteCheckedException {
        verify(BigDecimal.ZERO.setScale(-1, RoundingMode.HALF_UP));
        verify(BigDecimal.valueOf(Long.MAX_VALUE).setScale(-3, RoundingMode.HALF_DOWN));
        verify(BigDecimal.valueOf(Long.MIN_VALUE).setScale(-5, RoundingMode.HALF_EVEN));
        verify(BigDecimal.valueOf(Integer.MAX_VALUE).setScale(-8, RoundingMode.UP));
        verify(BigDecimal.valueOf(Integer.MIN_VALUE).setScale(-10, RoundingMode.DOWN));
        verify(BigDecimal.valueOf(Double.MAX_VALUE).setScale(-12, RoundingMode.CEILING));
        verify(BigDecimal.valueOf(Double.MIN_VALUE).setScale(-15, RoundingMode.FLOOR));
    }

    /** */
    @Test
    public void testString() throws IgniteCheckedException {
        verify("ascii0123456789");
        verify("的的abcdкириллица");
        verify(new String(new char[] {0xD800, '的', 0xD800, 0xD800, 0xDC00, 0xDFFF}));
        verify(new String(new char[] {0xD801, 0xDC37}));
    }

    /** */
    @Test
    public void testUuid() throws IgniteCheckedException {
        verify(UUID.randomUUID());
    }

    /** */
    @Test
    public void testIgniteUuid() throws IgniteCheckedException {
        verify(IgniteUuid.randomUuid());
    }

    /** */
    @Test
    public void testDate() throws IgniteCheckedException {
        verify(new Date());
    }

    /** */
    @Test
    public void testTimestamp() throws IgniteCheckedException {
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        ts.setNanos(999999999);

        verify(ts);
    }

    /** */
    @Test
    public void testTime() throws IgniteCheckedException {
        verify(new Time(System.currentTimeMillis()));
    }

    /** */
    @Test
    public void testTimeArray() throws IgniteCheckedException {
        verify(new Time[]{new Time(System.currentTimeMillis()), new Time(123456789)});
    }

    /** */
    @Test
    public void testByteArray() throws IgniteCheckedException {
        verify(new byte[] {10, 20, 30});
    }

    /** */
    @Test
    public void testShortArray() throws IgniteCheckedException {
        verify(new short[] {10, 20, 30});
    }

    /** */
    @Test
    public void testIntArray() throws IgniteCheckedException {
        verify(new int[] {10, 20, 30});
    }

    /** */
    @Test
    public void testLongArray() throws IgniteCheckedException {
        verify(new long[] {10, 20, 30});
    }

    /** */
    @Test
    public void testFloatArray() throws IgniteCheckedException {
        verify(new float[] {10.1f, 20.1f, 30.1f});
    }

    /** */
    @Test
    public void testDoubleArray() throws IgniteCheckedException {
        verify(new double[] {10.1d, 20.1d, 30.1d});
    }

    /** */
    @Test
    public void testCharArray() throws IgniteCheckedException {
        verify(new char[] {10, 20, 30});
    }

    /** */
    @Test
    public void testBooleanArray() throws IgniteCheckedException {
        verify(new boolean[] {true, false, true});
    }

    /** */
    @Test
    public void testDecimalArray() throws IgniteCheckedException {
        verify(new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.TEN});
    }

    /** */
    @Test
    public void testStringArray() throws IgniteCheckedException {
        verify(new String[] {"str1", "str2", "str3"});
    }

    /** */
    @Test
    public void testUuidArray() throws IgniteCheckedException {
        verify(new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()});
    }

    /** */
    @Test
    public void testDateArray() throws IgniteCheckedException {
        verify(new Date[] {new Date(11111), new Date(22222), new Date(33333)});
    }

    /** */
    @Test
    public void testObjectArray() throws IgniteCheckedException {
        verify(new Object[] {1, 2, 3});
    }

    /** */
    @Test
    public void testCollection() throws IgniteCheckedException {
        testCollection(new ArrayList<>(3));
        testCollection(new LinkedHashSet<>());
        testCollection(new HashSet<>());
        testCollection(new TreeSet<>());
        testCollection(new ConcurrentSkipListSet<>());
    }

    /** */
    private void testCollection(Collection<Integer> col) throws IgniteCheckedException {
        col.add(1);
        col.add(2);
        col.add(3);

        verify(col);
    }

    /** */
    @Test
    public void testMap() throws IgniteCheckedException {
        testMap(new HashMap<>());
        testMap(new LinkedHashMap<>());
        testMap(new TreeMap<>());
        testMap(new ConcurrentHashMap<>());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void testMap(Map<Integer, String> map) throws IgniteCheckedException {
        map.put(1, "str1");
        map.put(2, "str2");
        map.put(3, "str3");

        verify(map);
    }

    /** */
    @Test
    public void testEnum() throws IgniteCheckedException {
        verify(BinaryMarshallerSelfTest.TestEnum.B);
    }

    /** */
    @Test
    public void testDateAndTimestampInSingleObject() throws IgniteCheckedException {
        BinaryMarshallerSelfTest.DateClass1 obj1 = new BinaryMarshallerSelfTest.DateClass1();
        obj1.date = new Date();
        obj1.ts = new Timestamp(System.currentTimeMillis());
        obj1.time = new Time(System.currentTimeMillis());
        obj1.timeArr = new Time[]{obj1.time, new Time(obj1.date.getTime()), new Time(System.currentTimeMillis())};

        verify(obj1);
    }

    /** */
    @Test
    public void testVoid() throws IgniteCheckedException {
        verify(Void.class);
        verify(Void.TYPE);
    }

    /** */
    @Test
    public void testPojo() throws IgniteCheckedException {
        verify(new BinaryMarshallerSelfTest.Value(1));
    }

    /** */
    @Test
    public void testCustomSerialization1() throws IgniteCheckedException {
        verify(new BinaryMarshallerSelfTest.ObjectRaw(1, 2));
    }

    /** */
    @Test
    public void testCustomSerialization12() throws IgniteCheckedException {
        verify(new BinaryMarshallerSelfTest.ObjectWithRaw(1, 2));
    }

    /** */
    @Test
    public void testNotRegistredType() throws IgniteCheckedException {
        verify(new BinaryMarshallerSelfTest.Wrapper("42"));
    }

    /**
     * @return Binary marshaller.
     */
    protected BinaryMarshaller binaryMarshaller(byte protoVer) throws IgniteCheckedException {
        IgniteConfiguration iCfg = new IgniteConfiguration();

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setCompactFooter(compactFooter());
        bCfg.setProtocolVersion(protoVer);

        List<BinaryTypeConfiguration> cfgs = Arrays.asList(
            new BinaryTypeConfiguration(BinaryMarshallerSelfTest.Value.class.getName()),
            new BinaryTypeConfiguration(BinaryMarshallerSelfTest.ObjectRaw.class.getName()),
            new BinaryTypeConfiguration(BinaryMarshallerSelfTest.ObjectWithRaw.class.getName()),
            new BinaryTypeConfiguration(BinaryMarshallerSelfTest.SimpleObject.class.getName())
        );

        bCfg.setTypeConfigurations(cfgs);

        iCfg.setBinaryConfiguration(bCfg);
        iCfg.setClientMode(false);
        iCfg.setDiscoverySpi(new TcpDiscoverySpi() {
            @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                //No-op.
            }
        });

        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), iCfg, new NullLogger());

        BinaryMarshaller marsh = new BinaryMarshaller();

        List<String> excludedClasses = Collections.singletonList(BinaryMarshallerSelfTest.Wrapper.class.getName());

        MarshallerContextTestImpl marshCtx = new MarshallerContextTestImpl(null, excludedClasses);

        GridTestKernalContext kernCtx = new GridTestKernalContext(log, iCfg);
        kernCtx.add(new GridDiscoveryManager(kernCtx));

        marshCtx.onMarshallerProcessorStarted(kernCtx, null);

        marsh.setContext(marshCtx);

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);

        return marsh;
    }

    /**
     * @param obj Object.
     * @param marsh Marshaller.
     * @return Binary object.
     */
    protected <T> BinaryObjectImpl marshal(T obj, BinaryMarshaller marsh) throws IgniteCheckedException {
        byte[] bytes = marsh.marshal(obj);

        return new BinaryObjectImpl(U.<GridBinaryMarshaller>field(marsh, "impl").context(),
            bytes, 0);
    }

    /**
     * @return Whether to use compact footers or not.
     */
    protected boolean compactFooter() {
        return true;
    }
}
