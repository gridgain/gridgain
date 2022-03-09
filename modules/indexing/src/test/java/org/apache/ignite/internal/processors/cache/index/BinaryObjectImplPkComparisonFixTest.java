/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheDefaultBinaryAffinityKeyMapper;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.testframework.GridTestUtils;
import org.gridgain.internal.h2.util.Bits;
import org.gridgain.internal.h2.value.CompareMode;
import org.gridgain.internal.h2.value.Value;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/**
 *
 */
@RunWith(Parameterized.class)
public class BinaryObjectImplPkComparisonFixTest extends AbstractIndexingCommonTest {
    /** RNG for random strings. */
    private static final Random rnd = new Random(System.currentTimeMillis());

    /** Cache name. */
    private static final String CACHE_NAME = "GDD_ACC_MANDATE";

    /** Name of the {@code GridH2ValueCacheObject#disableBinaryPkCompareFix} field. */
    private static final String FIX_FIELD_NAME = "disableBinaryPkCompareFix";

    /** Flag value during data upload and initial indexes validation. */
    @Parameter
    public boolean disableFixOnUpload;

    /** Flag value for indexes validation after server restart. */
    @Parameter(1)
    public boolean disableFixOnRestart;

    /** All combinations of boolean pairs. */
    @Parameters(name = "disableFixOnUpload={0}, disableFixOnRestart={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {false, false},
            new Object[] {false, true},
            new Object[] {true, false},
            new Object[] {true, true}
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalSegments(3)
                .setWalSegmentSize(512 * 1024)
                .setPageSize(16384)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    /**
     * Tests that switching flag value affects index validation output in case of possibly corrupted PK index.
     * Test creates a cache with SQL indexes and inserts two key/value pairs in it. These keys are specifically chosen
     * to have same hash value and to corrupt the PK index. Particularly,
     * {@link BinaryObjectImpl#compare(Object, Object)} and {@link Bits#compareNotNullSigned(byte[], byte[])} would
     * return different results on these keys in {@link GridH2ValueCacheObject#compareTypeSafe(Value, CompareMode)},
     * which leads to issues in index validation closure, and unexpected node crashes after Ignite version update.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidateIndexesAfterRestart() throws Exception {
        assertNotNull(GridH2ValueCacheObject.class.getDeclaredField(FIX_FIELD_NAME));

        TestKey key1 = new TestKey(
            "9KkofUymNkX1HlNt89OqSPcJCNys",
            "UUn43TNrUDT1ip0eM1cc0tgVcFxW",
            "iRnj3JcTbmDUWnF49AcHSptMBUz"
        );

        TestKey key2 = new TestKey(
            "bhj4CF5sQxj6llPYrph0OM64",
            "I116KgWfT57dgdxehFWs05ae",
            "2QcyVgp8LMMZGug"
        );

        setFieldValue(null, GridH2ValueCacheObject.class, FIX_FIELD_NAME, disableFixOnUpload);

        try {
            IgniteEx srv = startGrid(0);

            srv.cluster().state(ClusterState.ACTIVE);

            IgniteEx client = startClientGrid(1);

            IgniteCache<TestKey, TestValue> cache = client.createCache(createCacheConfiguration());

            cache.put(key1, val(key1));
            cache.put(key2, val(key2));

            VisorValidateIndexesJobResult res = validateIndexes(srv);

            assertFalse(res.toString(), res.hasIssues());

            stopAllGrids(false);
        }
        finally {
            setFieldValue(null, GridH2ValueCacheObject.class, FIX_FIELD_NAME, false);
        }

        setFieldValue(null, GridH2ValueCacheObject.class, FIX_FIELD_NAME, disableFixOnRestart);

        try {
            IgniteEx srv = startGrid(0);

            startClientGrid(1); // Closure doesn't work without this client for some reason...

            VisorValidateIndexesJobResult res = validateIndexes(srv);

            assertEquals(res.toString(), disableFixOnUpload != disableFixOnRestart, res.hasIssues());
        }
        finally {
            setFieldValue(null, GridH2ValueCacheObject.class, FIX_FIELD_NAME, false);
        }
    }

    /**
     * Invokes validate indexes closure and returnes the result.
     */
    private VisorValidateIndexesJobResult validateIndexes(IgniteEx srv) throws Exception {
        ValidateIndexesClosure clo = new ValidateIndexesClosure(
            () -> false,
            singleton(CACHE_NAME),
            0,
            0,
            false,
            true
        );

        srv.context().resource().injectGeneric(clo);

        return clo.call();
    }

    /**
     * @return Cache configuration for the test. Ensures that there are SQL indexes configured for the cache.
     */
    private CacheConfiguration<TestKey, TestValue> createCacheConfiguration() {
        CacheConfiguration<TestKey, TestValue> ccfg = new CacheConfiguration<>();

        ccfg.setName(CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(true,1024));

        ccfg.setSqlIndexMaxInlineSize(10);

        Set<String> keyFields = new HashSet<>();

        keyFields.add("STRKEY1");
        keyFields.add("STRKEY2");
        keyFields.add("STRKEY3");
        keyFields.add("AFFINITY_KEY");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("STRKEY1", "java.lang.String");
        fields.put("STRKEY2", "java.lang.String");
        fields.put("STRKEY3", "java.lang.String");
        fields.put("AFFINITY_KEY", "java.lang.Integer");

        QueryIndex qryIdx =
            new QueryIndex("STRKEY1", QueryIndexType.SORTED, false, "STRKEY1_ASC_IDX");

        QueryEntity qryEntity = new QueryEntity().setTableName("ACCFORECASTMANDATE")
            .setKeyType(TestKey.class.getName())
            .setKeyFields(keyFields)
            .setFields(fields)
            .setValueType(TestValue.class.getName())
            .setIndexes(singleton(qryIdx));

        ccfg.setQueryEntities(singletonList(qryEntity));

        ccfg.setStatisticsEnabled(true);

        ccfg.setReadFromBackup(true);

        ccfg.setCopyOnRead(true);

        ccfg.setAffinityMapper(new CacheDefaultBinaryAffinityKeyMapper(
            new CacheKeyConfiguration[] {
                new CacheKeyConfiguration(TestKey.class.getName(), "affinityKey")
            }
        ));

        return ccfg;
    }

    /**
     * Creates a value for the key.
     */
    private static TestValue val(TestKey key) {
        TestValue val = new TestValue();

        val.strKey1 = key.strKey1;
        val.strKey2 = key.strKey2;
        val.strKey3 = key.strKey3;

        val.value = GridTestUtils.randomString(rnd, 10);

        return val;
    }

    /**
     * Key class. Class structure is taken from the real case and a bigger reproducer program for it.
     */
    public static class TestKey {
        /** */
        private final String strKey1;

        /** */
        private final String strKey2;

        /** */
        private final String strKey3;

        /** */
        @SuppressWarnings("unused")
        @AffinityKeyMapped
        private final int affinityKey;

        /** */
        public TestKey(String strKey1, String strKey2, String strKey3) {
            this.strKey1 = strKey1;
            this.strKey2 = strKey2;
            this.strKey3 = strKey3;

            affinityKey = hashString(strKey1);
        }

        /** */
        private int hashString(String toHash) {
            int h;
            return (toHash == null) ? 0 : (h = toHash.hashCode()) ^ (h >>> 16);
        }
    }

    /**
     * Value class. Has only one extra field, it's enough for the test.
     */
    @SuppressWarnings("unused")
    public static class TestValue {
        /** */
        private String strKey1;

        /** */
        private String strKey2;

        /** */
        private String strKey3;

        /** */
        private String value;
    }
}
