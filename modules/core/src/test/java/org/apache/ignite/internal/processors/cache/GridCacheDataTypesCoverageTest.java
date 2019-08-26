/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicyFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Data types coverage for basic cache operations.
 */
@RunWith(Parameterized.class)
public class GridCacheDataTypesCoverageTest extends GridCommonAbstractTest {
    /** */
    @SuppressWarnings("unchecked")
    private static final Factory[] TTL_FACTORIES = {
        null,
        new FactoryBuilder.SingletonFactory<ExpiryPolicy>(new EternalExpiryPolicy()) {
            @Override public String toString() {
                return "EternalExpiryPolicy";
            }
        },
        new FactoryBuilder.SingletonFactory(new TestPolicy(60_000L, 61_000L, 62_000L)){
            @Override public String toString() {
                return "ExpiryPolicy:60_000L, 61_000L, 62_000L";
            }
        }};

    /** */
    private static final Factory[] EVICTION_FACTORIES = {
        null,
        new FifoEvictionPolicyFactory(10, 1, 0) {
            @Override public String toString() {
                return "FifoEvictionPolicyFactory";
            }
        }
    };

    // TODO: 22.08.19 SortedEvictionPolicyFactory fail test, research and ignore with todo.
//    /** */
//    private static final Factory[] EVICTION_FACTORIES = {
//        null,
//        new FifoEvictionPolicyFactory(10, 1, 0) {
//            @Override public String toString() {
//                return "FifoEvictionPolicyFactory";
//            }
//        },
//        new SortedEvictionPolicyFactory(10, 1, 0) {
//            @Override public String toString() {
//                return "SortedEvictionPolicyFactory";
//            }
//        }
//    };


    /** Possible options for enabled/disabled properties like onheapCacheEnabled */
    private static final boolean[] BOOLEANS = {true, false};

    /** */
    private static final int NODES_CNT = 3;

    /** */
    private static final int TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE = 2_000;

    /**
     * Here's an id of specific set of params is stored. It's used to overcome the limitations of junit4.11 in order to
     * pseudo-run @Before method only once after applying specific set of parameters. See {@code init()} for more
     * details.
     */
    private static UUID prevParamLineId;

    /** */
    @Parameterized.Parameter
    public UUID paramLineId;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(2)
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(3)
    public Factory<? extends ExpiryPolicy> ttlFactory;

    /** */
    @Parameterized.Parameter(4)
    public int backups;

    /** */
    @Parameterized.Parameter(5)
    public Factory evictionFactory;

    /** */
    @Parameterized.Parameter(6)
    public boolean onheapCacheEnabled;

    /** */
    @Parameterized.Parameter(7)
    public CacheWriteSynchronizationMode writeSyncMode;

    /** */
    @Parameterized.Parameter(8)
    public boolean persistenceEnabled;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "atomicityMode={1}, cacheMode={2}, ttlFactory={3}, backups={4}," +
        " evictionFactory={5}, onheapCacheEnabled={6}, writeSyncMode={7}, persistenceEnabled={8}")
    public static Collection parameters() {
        Set<Object[]> params = new HashSet<>();

        Object[] baseParamLine = {null, CacheAtomicityMode.ATOMIC, CacheMode.PARTITIONED, null, 2, null,
            false, CacheWriteSynchronizationMode.FULL_SYNC, true};

        Object[] paramLine = null;

        for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[1] = atomicityMode;

            params.add(paramLine);
        }

        for (CacheMode cacheMode : CacheMode.values()) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[2] = cacheMode;

            params.add(paramLine);
        }

        assert paramLine != null;

        if ((paramLine[1]) != CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT) {
            for (Factory ttlFactory : TTL_FACTORIES) {
                paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

                paramLine[3] = ttlFactory;

                params.add(paramLine);
            }
        }

        for (int backups : new int[] {0, 1, 2}) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[4] = backups;

            params.add(paramLine);
        }

        for (Factory evictionFactory : EVICTION_FACTORIES) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[5] = evictionFactory;

            params.add(paramLine);
        }

        for (Boolean onheapCacheEnabled : BOOLEANS) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[6] = onheapCacheEnabled;

            params.add(paramLine);
        }

        for (CacheWriteSynchronizationMode writeSyncMode : CacheWriteSynchronizationMode.values()) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[7] = writeSyncMode;

            params.add(paramLine);
        }

        for (boolean persistenceEnabled : BOOLEANS) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[8] = persistenceEnabled;

            params.add(paramLine);
        }

        for (Object[] pLine: params)
            pLine[0] = UUID.randomUUID();

        return params;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (persistenceEnabled) {
            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            cfg.setDataStorageConfiguration(storageCfg);
        }

        return cfg;
    }

    /**
     * For any new set of parameters prepare new env:
     * <ul>
     * <li>stop all grids;</li>
     * <li>clean persistence dir;</li>
     * <li>start grids;</li>
     * <li>Update {@code prevParamLineId} with new value in order to process {@code init()} only once for specific set
     * of parameters but not once for every test method. That should be refactored after migration to junit 5 or junit
     * 4.13</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Before
    public void init() throws Exception {
        if (!paramLineId.equals(prevParamLineId)) {
            stopAllGrids();

            cleanPersistenceDir();

            startGridsMultiThreaded(NODES_CNT);

            prevParamLineId = paramLineId;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testByteDataType() throws Exception {
//        checkBasicCacheOperations(
//            Byte.MIN_VALUE,
//            Byte.MAX_VALUE,
//            0,
//            1);
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testShortDataType() throws Exception {
//        checkBasicCacheOperations(
//            Short.MIN_VALUE,
//            Short.MAX_VALUE,
//            0,
//            1);
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testIntegerDataType() throws Exception {
//        checkBasicCacheOperations(
//            Integer.MIN_VALUE,
//            Integer.MAX_VALUE,
//            0,
//            1);
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testLongDataType() throws Exception {
//        checkBasicCacheOperations(
//            Long.MIN_VALUE,
//            Long.MAX_VALUE,
//            0,
//            1);
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testFloatDataType() throws Exception {
//        checkBasicCacheOperations(
//            Float.MIN_VALUE,
//            Float.MAX_VALUE,
//            Float.NaN,
//            Float.NEGATIVE_INFINITY,
//            Float.POSITIVE_INFINITY,
//            0,
//            0.0,
//            1,
//            1.1);
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testDoubleDataType() throws Exception {
//        checkBasicCacheOperations(
//            Double.MIN_VALUE,
//            Double.MAX_VALUE,
//            Double.NaN,
//            Double.NEGATIVE_INFINITY,
//            Double.POSITIVE_INFINITY,
//            0,
//            0.0,
//            1,
//            1.1);
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testBooleanDataType() throws Exception {
//        checkBasicCacheOperations(
//            Boolean.TRUE,
//            Boolean.FALSE);
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testCharacterDataType() throws Exception {
//        checkBasicCacheOperations(
//            'a',
//            'A',
//            0);
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testStringDataType() throws Exception {
//        checkBasicCacheOperations(
//            "aAbB",
//            "");
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testByteArrayDataType() throws Exception {
//        checkBasicCacheOperations(
//            new byte[] {},
//            new byte[] {3, 2, 1});
//        // TODO: 23.08.19 new Byte[]{}, new Byte[] {1, 2, 3} fails with unexpected return type.
////        checkBasicCacheOperations(new Byte[]{}, new Byte[] {1, 2, 3}, new byte[] {3, 2, 1});
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testObjectArrayDataType() throws Exception {
//        checkBasicCacheOperations(
//            new Object[]{},
//            new Object[] {"String", Boolean.TRUE, 'A', 1},
//            new Object[] {"String",
//                new ObjectBasedOnPrimitives(123, 123.123, true)});
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testListDataType() throws Exception {
//        checkBasicCacheOperations(
//            new ArrayList<>(),
//            (Serializable)Collections.singletonList("Aaa"),
//            (Serializable)Arrays.asList("String", Boolean.TRUE, 'A', 1));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testSetDataType() throws Exception {
//        checkBasicCacheOperations(
//            new HashSet<>(),
//            (Serializable)Collections.singleton("Aaa"),
//            new HashSet<>(Arrays.asList("String", Boolean.TRUE, 'A', 1)));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testQueueDataType() throws Exception {
////        ArrayBlockingQueue<Integer> queueToCheck = new ArrayBlockingQueue<>(5);
////        queueToCheck.addAll(Arrays.asList(1, 2, 3));
//
//        // TODO: 22.08.19 In case of using ArrayBlockingQueue as key, cache.get() returns null.
////        checkBasicCacheOperations(new LinkedList<>(), new LinkedList<>(Arrays.asList("Aaa", "Bbb")), queueToCheck);
//
//        checkBasicCacheOperations(
//            new LinkedList<>(),
//            new LinkedList<>(Arrays.asList("Aaa", "Bbb")));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testObjectBasedOnPrimitivesDataType() throws Exception {
//        checkBasicCacheOperations(
//            new ObjectBasedOnPrimitives(0, 0.0, false),
//            new ObjectBasedOnPrimitives(123, 123.123, true));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testObjectBasedOnPrimitivesAndCollectionsDataType() throws Exception {
//        checkBasicCacheOperations(
//            new ObjectBasedOnPrimitivesAndCollections(0,
//                Collections.emptyList(),
//                new boolean[] {}),
//            new ObjectBasedOnPrimitivesAndCollections(123,
//                Arrays.asList(1.0, 0.0),
//                new boolean[] {true, false}));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testObjectBasedOnPrimitivesAndCollectionsAndNestedObjectsDataType() throws Exception {
//        checkBasicCacheOperations(
//            new ObjectBasedOnPrimitivesCollectionsAndNestedObject(0,
//                Collections.emptyList(),
//                new ObjectBasedOnPrimitivesAndCollections(
//                    0,
//                    Collections.emptyList(),
//                    new boolean[] {})),
//            new ObjectBasedOnPrimitivesCollectionsAndNestedObject(-1,
//                Collections.singletonList(0.0),
//                new ObjectBasedOnPrimitivesAndCollections(
//                    Integer.MAX_VALUE,
//                    new ArrayList<>(Collections.singleton(22.2)),
//                    new boolean[] {true, false, true})));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testDateDataType() throws Exception {
//        checkBasicCacheOperations(
//            new Date(),
//            new Date(Long.MIN_VALUE),
//            new Date(Long.MAX_VALUE));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testSqlDateDataType() throws Exception {
//        checkBasicCacheOperations(
//            new java.sql.Date(Long.MIN_VALUE),
//            new java.sql.Date(Long.MAX_VALUE));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testCalendarDataType() throws Exception {
//        checkBasicCacheOperations(new GregorianCalendar());
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testInstantDataType() throws Exception {
//        checkBasicCacheOperations(
//            Instant.now(),
//            Instant.ofEpochMilli(Long.MIN_VALUE),
//            Instant.ofEpochMilli(Long.MAX_VALUE));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testLocalDateDataType() throws Exception {
//        checkBasicCacheOperations(
//            LocalDate.of(2015, 2, 20),
//            LocalDate.now().plusDays(1));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testLocalDateTimeDataType() throws Exception {
//        checkBasicCacheOperations(
//            LocalDateTime.of(2015, 2, 20, 9, 4, 30),
//            LocalDateTime.now().plusDays(1));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testLocalTimeDataType() throws Exception {
//        checkBasicCacheOperations(
//            LocalTime.of(9, 4, 40),
//            LocalTime.now());
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testBigIntegerDataType() throws Exception {
//        checkBasicCacheOperations(
//            new BigInteger("1"),
//            BigInteger.ONE,
//            BigInteger.ZERO,
//            new BigInteger("123456789"));
//    }
//
//    /**
//     *
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testBigDecimalDataType() throws Exception {
//        checkBasicCacheOperations(
//            new BigDecimal(123.123),
//            BigDecimal.ONE,
//            BigDecimal.ZERO,
//            BigDecimal.valueOf(123456789, 0),
//            BigDecimal.valueOf(123456789, 1),
//            BigDecimal.valueOf(123456789, 2),
//            BigDecimal.valueOf(123456789, 3));
//    }

    /**
     * Verification that following cache methods works correctly:
     *
     * <ul>
     * <li>put</li>
     * <li>putAll</li>
     * <li>remove</li>
     * <li>removeAll</li>
     * <li>get</li>
     * <li>getAll</li>
     * </ul>
     *
     * @param valsToCheck Array of values to check.
     */
    @SuppressWarnings("unchecked")
    private void checkBasicCacheOperations(Serializable... valsToCheck) throws Exception {
        assert valsToCheck.length > 0;

        // In case of BigDecimal, cache internally changes bitLength of BigDecimal's intValue,
        // so that EqualsBuilder.reflectionEquals returns false.
        // As a result in case of BigDecimal data type Objects.equals is used.
        // Same is about BigInteger.
        BiFunction<Object, Object, Boolean> equalsProcessor =
            valsToCheck[0] instanceof BigDecimal || valsToCheck[0] instanceof BigInteger ?
                Objects::equals :
                (lhs, rhs) -> EqualsBuilder.reflectionEquals(
                    lhs, rhs, false, lhs.getClass(), true);

        String cacheName = "cache" + UUID.randomUUID();

        IgniteCache<Object, Object> cache = grid(new Random().nextInt(NODES_CNT)).createCache(
            new CacheConfiguration<>()
                .setName(cacheName)
                .setAtomicityMode(atomicityMode)
                .setCacheMode(cacheMode)
                .setExpiryPolicyFactory(ttlFactory)
                .setBackups(backups)
                .setEvictionPolicyFactory(evictionFactory)
                .setOnheapCacheEnabled(evictionFactory != null || onheapCacheEnabled)
                .setWriteSynchronizationMode(writeSyncMode));

        Map<Serializable, Serializable> keyValMap = new HashMap<>();

        for (int i = 0; i < valsToCheck.length; i++)
            keyValMap.put(valsToCheck[i], valsToCheck[valsToCheck.length - i - 1]);

        for (Map.Entry<Serializable, Serializable> keyValEntry : keyValMap.entrySet()) {
            // Put.
            cache.put(keyValEntry.getKey(), keyValEntry.getValue());

            Serializable clonedKey = SerializationUtils.clone(keyValEntry.getKey());

            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> cache.get(clonedKey) != null, TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Unable to retrieve value for key = [" + clonedKey + "].");

            // Check Put/Get.
            assertTrue(equalsProcessor.apply(keyValEntry.getValue(), cache.get(clonedKey)));

            // Remove.
            cache.remove(clonedKey);

            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> cache.get(clonedKey) == null, TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Unable to retrieve null value for key = [" + clonedKey + "] after entry removal." );

            // Check remove.
            assertNull(cache.get(clonedKey));
        }

        // PutAll
        cache.putAll(keyValMap);

        // Check putAll.
        for (Map.Entry<Serializable, Serializable> keyValEntry : keyValMap.entrySet()) {
            Serializable clonedKey = SerializationUtils.clone(keyValEntry.getKey());

            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> cache.get(clonedKey) != null, TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Unable to retrieve value for key = [" + clonedKey + "].");

            assertTrue(equalsProcessor.apply(keyValEntry.getValue(), cache.get(clonedKey)));
        }

        Set<Serializable> clonedKeySet =
            keyValMap.keySet().stream().map(SerializationUtils::clone).collect(Collectors.toSet());

        if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
            !waitForCondition(() -> cache.getAll(clonedKeySet) != null, TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
            fail("Unable to retrieve values value for keySet = [" + clonedKeySet + "]." );

        // Check get all.
        Map<Object, Object> mapToCheck = cache.getAll(clonedKeySet);

        assertEquals(keyValMap.size(), mapToCheck.size());

        for (Map.Entry<Serializable, Serializable> keyValEntry : keyValMap.entrySet()) {
            boolean keyFound = false;
            for (Map.Entry<Object, Object> keyValEntryToCheck : mapToCheck.entrySet()) {
                if (equalsProcessor.apply(keyValEntry.getKey(), keyValEntryToCheck.getKey())) {
                    keyFound = true;

                    assertTrue(equalsProcessor.apply(keyValEntry.getValue(), keyValEntryToCheck.getValue()));

                    break;
                }
            }
            assertTrue(keyFound);
        }

        // Remove all.
        cache.removeAll(clonedKeySet);

        // Check remove all.
        for (Serializable clonedKey : clonedKeySet) {
            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> cache.get(clonedKey) == null, TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Unable to retrieve null value for key = [" + clonedKey + "] after entry removal." );

            assertNull(cache.get(clonedKey));
        }
    }

    /**
     *
     */
    private static class TestPolicy implements ExpiryPolicy, Serializable {
        /** */
        private Long create;

        /** */
        private Long access;

        /** */
        private Long update;

        /**
         * @param create TTL for creation.
         * @param access TTL for access.
         * @param update TTL for update.
         */
        TestPolicy(@Nullable Long create,
            @Nullable Long update,
            @Nullable Long access) {
            this.create = create;
            this.update = update;
            this.access = access;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForCreation() {
            return create != null ? new Duration(TimeUnit.MILLISECONDS, create) : null;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForAccess() {
            return access != null ? new Duration(TimeUnit.MILLISECONDS, access) : null;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForUpdate() {
            return update != null ? new Duration(TimeUnit.MILLISECONDS, update) : null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestPolicy.class, this);
        }
    }

    /**
     * Objects based on primitives only.
     */
    @SuppressWarnings("unused")
    private static class ObjectBasedOnPrimitives implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int intField;

        /** */
        private double doubleField;

        /** */
        private boolean booleanField;

        /**
         * @param intField Int field.
         * @param doubleField Double field.
         * @param booleanField Boolean field.
         */
        ObjectBasedOnPrimitives(int intField, double doubleField, boolean booleanField) {
            this.intField = intField;
            this.doubleField = doubleField;
            this.booleanField = booleanField;
        }

        /**
         * @return Int field.
         */
        public int intField() {
            return intField;
        }

        /**
         * @param intField New int field.
         */
        public void intField(int intField) {
            this.intField = intField;
        }

        /**
         * @return Double field.
         */
        public double doubleField() {
            return doubleField;
        }

        /**
         * @param doubleField New double field.
         */
        public void doubleField(double doubleField) {
            this.doubleField = doubleField;
        }

        /**
         * @return Boolean field.
         */
        public boolean booleanField() {
            return booleanField;
        }

        /**
         * @param booleanField New boolean field.
         */
        public void booleanField(boolean booleanField) {
            this.booleanField = booleanField;
        }
    }

    /**
     * Objects based on primitives and collections.
     */
    @SuppressWarnings("unused")
    private static class ObjectBasedOnPrimitivesAndCollections implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int intField;

        /** */
        private List<Double> doubleListField;

        /** */
        private boolean[] booleanArrField;

        /**
         * @param intField Int field.
         * @param doubleListField Double list field.
         * @param booleanArrField Boolean array field.
         */
        ObjectBasedOnPrimitivesAndCollections(int intField, List<Double> doubleListField,
            boolean[] booleanArrField) {
            this.intField = intField;
            this.doubleListField = doubleListField;
            this.booleanArrField = booleanArrField;
        }

        /**
         * @return Int field.
         */
        public int intField() {
            return intField;
        }

        /**
         * @param intField New int field.
         */
        public void intField(int intField) {
            this.intField = intField;
        }

        /**
         * @return Double list field.
         */
        public List<Double> doubleListField() {
            return doubleListField;
        }

        /**
         * @param doubleListField New double list field.
         */
        public void doubleListField(List<Double> doubleListField) {
            this.doubleListField = doubleListField;
        }

        /**
         * @return Boolean array field.
         */
        public boolean[] booleanArrayField() {
            return booleanArrField;
        }

        /**
         * @param booleanArrField New boolean array field.
         */
        public void booleanArrayField(boolean[] booleanArrField) {
            this.booleanArrField = booleanArrField;
        }
    }

    /**
     * Objects based on primitives collections and nested objects.
     */
    @SuppressWarnings("unused")
    private static class ObjectBasedOnPrimitivesCollectionsAndNestedObject implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int intField;

        /** */
        private List<Double> doubleListField;

        /** */
        private ObjectBasedOnPrimitivesAndCollections nestedObjField;

        /**
         * @param intField Int field.
         * @param doubleListField Double list field.
         * @param nestedObjField Nested object field.
         */
        ObjectBasedOnPrimitivesCollectionsAndNestedObject(int intField, List<Double> doubleListField,
            ObjectBasedOnPrimitivesAndCollections nestedObjField) {
            this.intField = intField;
            this.doubleListField = doubleListField;
            this.nestedObjField = nestedObjField;
        }

        /**
         * @return Int field.
         */
        public int intField() {
            return intField;
        }

        /**
         * @param intField New int field.
         */
        public void intField(int intField) {
            this.intField = intField;
        }

        /**
         * @return Double list field.
         */
        public List<Double> doubleListField() {
            return doubleListField;
        }

        /**
         * @param doubleListField New double list field.
         */
        public void doubleListField(List<Double> doubleListField) {
            this.doubleListField = doubleListField;
        }

        /**
         * @return Nested object field.
         */
        public ObjectBasedOnPrimitivesAndCollections nestedObjectField() {
            return nestedObjField;
        }

        /**
         * @param nestedObjField New nested object field.
         */
        public void nestedObjectField(ObjectBasedOnPrimitivesAndCollections nestedObjField) {
            this.nestedObjField = nestedObjField;
        }
    }
}
