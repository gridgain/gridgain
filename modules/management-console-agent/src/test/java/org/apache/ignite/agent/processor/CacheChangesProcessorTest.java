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

package org.apache.ignite.agent.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.agent.dto.cache.CacheInfo;
import org.apache.ignite.agent.dto.cache.CacheSqlIndexMetadata;
import org.apache.ignite.agent.dto.cache.CacheSqlMetadata;
import org.apache.ignite.agent.fixtures.Country;
import org.apache.ignite.agent.fixtures.CountryWithAnnotations;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;

import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterCachesInfoDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterCachesSqlMetaDest;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Cache changes processor test.
 */
public class CacheChangesProcessorTest extends AgentCommonAbstractTest {
    /**
     * Should send initial states to backend.
     */
    @Test
    public void shouldSendInitialStates() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            CacheInfo actual = F.find(cacheInfos, null, (P1<CacheInfo>)i -> CU.isSystemCache(i.getCacheName()));

            return cacheInfos.size() == 1 && actual != null && actual.isSystemCache();
        });
        assertWithPoll(() -> interceptor.getPayload(buildClusterCachesSqlMetaDest(cluster.id())) != null);
    }

    /** */
    @Test
    public void shouldSendValidCacheInfo() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.getOrCreateCache("test-cache");

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            CacheInfo actual = F.find(cacheInfos, null,
                (P1<CacheInfo>)i -> "test-cache".equals(i.getCacheName()));

            return actual != null &&
                CU.cacheId("test-cache") == actual.getCacheId() &&
                !actual.isSystemCache() &&
                !actual.isCreatedBySql();
        });

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE mc_agent_test_table_1 (id int, value int, PRIMARY KEY (id));"),
            true
        );

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            CacheInfo actual = F.find(cacheInfos, null,
                (P1<CacheInfo>)i -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_1".equals(i.getCacheName()));

            return actual != null &&
                CU.cacheId("SQL_PUBLIC_MC_AGENT_TEST_TABLE_1") == actual.getCacheId() &&
                !actual.isSystemCache() &&
                actual.isCreatedBySql();
        });
    }

    /**
     * GG-26556 Testcase 4:
     *
     * 1. Start 1 ignite node.
     * 2. Create a cache.
     * 3. Wait for 1 second until message with cache info will be send to GMC.
     * 4. Verify that cache info list contains the cache created.
     * 5. Destroy the cache.
     * 6. Wait 1 second until message with cache info will be send to GMC.
     * 7. Verify that cache info list does not contain the cache anymore.
     */
    @Test
    public void shouldSendCacheInfoOnCreatedOrDestroyedCache() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("test-cache");

        cache.put(1, 2);

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().anyMatch(i -> "test-cache".equals(i.getCacheName()));
        });

        cache.destroy();

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().noneMatch(i -> "test-cache".equals(i.getCacheName()));
        });
    }

    /**
     * GG-26556 Testcase 5:
     *
     * 1. Start 2 ignite node.
     * 2. Create a cache on second node.
     * 3. Verify the message with cache info is sent to GMC in less than 1 second.
     * 4. Verify that cache info list contains the cache created.
     * 5. Destroy the cache.
     * 6. Wait for 1 second until message with cache info will be sent to GMC.
     * 7. Verify that cache info list does not contains the cache anymore.
     */
    @Test
    public void shouldSendCacheInfoIfCacheCreatedOnOtherNode() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        IgniteEx ignite_2 = startGrid(0);

        IgniteCache<Object, Object> cache = ignite_2.getOrCreateCache("test-cache-1");

        cache.put(1, 2);

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().anyMatch(i -> "test-cache-1".equals(i.getCacheName()));
        });

        cache.destroy();

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().noneMatch(i -> "test-cache-1".equals(i.getCacheName()));
        });
    }

    /**
     * GG-26556 Testcase 6:
     *
     * 1. Start 1 ignite nodes.
     * 2. Execute SQL query: "CREATE TABLE mc_agent_test_table_1 (id int, value int, PRIMARY KEY (id));"
     * 3. Wait 1 second until message with cache info will be send to GMC.
     * 4. Verify that cache info list contains cache with “SQL_PUBLIC_MC_AGENT_TEST_TABLE_1” name.
     * 5. Execute SQL query: "DROP TABLE mc_agent_test_table_1;"
     * 6. Wait for 1 second until message with cache info will be sent to GMC.
     * 7. Verify that cache info list does not contain cache with “SQL_PUBLIC_MC_AGENT_TEST_TABLE_1” name.
     */
    @Test
    public void shouldSendCacheInfoOnCreatedOrDestroyedCacheFromSql() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE mc_agent_test_table_1 (id int, value int, PRIMARY KEY (id));"),
            true
        );

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().anyMatch(i -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_1".equals(i.getCacheName()));
        });

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("DROP TABLE mc_agent_test_table_1;"),
            true
        );

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().noneMatch(i -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_1".equals(i.getCacheName()));
        });
    }

    /**
     * GG-26556 Testcase 7:
     *
     * 1. Start 1 ignite node.
     * 2. Execute SQL query: "CREATE TABLE mc_agent_test_table_2 (id int, value int, PRIMARY KEY (id));"
     * 3. Wait for 1 second until message with cache metadata will be sent to GMC.
     * 4. Verify that cache metadata list contains metadata for cache with “*SQL_PUBLIC_MC_AGENT_TEST_TABLE_2*” name.
     * 5. Verify that cache metadata contains ID and VALUE fields.
     * 6. Execute SQL query: "ALTER TABLE mc_agent_test_table_2 ADD id_2 int;"
     * 7. Wait for 1 second until message with cache metadata will be sent to GMC.
     * 8. Verify that cache metadata list contains metadata for cache with “*SQL_PUBLIC_MC_AGENT_TEST_TABLE_2*” name.
     * 9. Verify that cache metadata contains ID, VALUE and ID_2 fields.
     */
    @Test
    public void shouldSendCacheMetadataOnAlterTableAndCreateIndex() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE mc_agent_test_table_2 (id int, value int, PRIMARY KEY (id));"),
            true
        );

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_2".equals(m.getCacheName()))
                .findFirst()
                .get();

            Map<String, String> fields = cacheMeta.getFields();

            return cacheMeta != null && fields.containsKey("ID") && fields.containsKey("VALUE");
        });

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("ALTER TABLE mc_agent_test_table_2 ADD id_2 int;"),
            true
        );

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_2".equals(m.getCacheName()))
                .findFirst()
                .get();

            Map<String, String> fields = cacheMeta.getFields();

            return cacheMeta != null && fields.containsKey("ID") && fields.containsKey("VALUE") && fields.containsKey("ID_2");
        });
    }

    /**
     * GG-26556 Testcase 8:
     *
     * 1.  Start 1 ignite node.
     * 2.  Execute SQL query: "CREATE TABLE mc_agent_test_table_3 (id int, value int, PRIMARY KEY (id));"
     * 3.  Wait for 1 second until message with cache metadata will be sent to GMC.
     * 4.  Verify that cache metadata list contains metadata for cache with “SQL_PUBLIC_MC_AGENT_TEST_TABLE_3” name.
     * 5.  Verify that cache metadata contains empty index list.
     * 6.  Execute SQL query: "CREATE INDEX my_index ON mc_agent_test_table_3 (value);"
     * 7.  Wait for 1 second until message with cache metadata will be sent to GMC.
     * 8.  Verify that cache metadata list contains metadata for cache with “SQL_PUBLIC_MC_AGENT_TEST_TABLE_3” name.
     * 9.  Verify that cache metadata contains index list with 1 element.
     * 10. Execute SQL query: "DROP INDEX my_index;"
     * 11. Verify that cache metadata list contains metadata for cache with “SQL_PUBLIC_MC_AGENT_TEST_TABLE_3” name.
     * 12. Verify that cache metadata contains empty index list.
     */
    @Test
    public void shouldSendCacheMetadataOnCreateAndDropIndex() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE mc_agent_test_table_3 (id int, value int, PRIMARY KEY (id));"),
            true
        );

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_3".equals(m.getCacheName()))
                .findFirst()
                .get();

            List<CacheSqlIndexMetadata> idxes = cacheMeta.getIndexes();

            return cacheMeta != null && idxes.isEmpty();
        });

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE INDEX my_index ON mc_agent_test_table_3 (value)"),
            true
        );

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_3".equals(m.getCacheName()))
                .findFirst()
                .get();

            List<CacheSqlIndexMetadata> idxes = cacheMeta.getIndexes();

            return cacheMeta != null && idxes.size() == 1;
        });

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("DROP INDEX my_index;"),
            true
        );

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_3".equals(m.getCacheName()))
                .findFirst()
                .get();

            List<CacheSqlIndexMetadata> idxes = cacheMeta.getIndexes();

            return cacheMeta != null && idxes.isEmpty();
        });
    }

    /**
     * GG-26556 Testcase 1:
     *
     * 1. Start 1 ignite node.
     * 2. Create cache by: org.apache.ignite.agent.processor.CacheChangesProcessorTest#getCountryCacheConfig()
     * 3. Wait for 1 second until message with cache info will be sent to GMC.
     * 4. Verify that cache info list contains cache with “Country” name.
     * 5. Wait for 1 second until message with cache sql metadata list will be sent to GMC.
     * 6. Verify that cache sql metadata list contains metadata for “Country” cache.
     * 7. Verify that cache sql metadata contains fields: ID, NAME, POPULATION with appropriate types (java.lang.Integer, java.lang.String, java.lang.Integer)
     */
    @Test
    public void shouldSendSqlMetadataForCacheCreatedByCacheConfiguration() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.getOrCreateCache(getCountryCacheConfig());

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().anyMatch(i -> "Country".equals(i.getCacheName()));
        });

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "Country".equals(m.getCacheName()))
                .findFirst()
                .get();

            return cacheMeta != null &&
                "java.lang.Integer".equals(cacheMeta.getFields().get("ID")) &&
                "java.lang.String".equals(cacheMeta.getFields().get("NAME")) &&
                "java.lang.Integer".equals(cacheMeta.getFields().get("POPULATION"));
        });
    }

    /**
     * GG-26556 Testcase 2:
     *
     * 1. Start 1 ignite node.
     * 2. Create cache by: org.apache.ignite.agent.processor.CacheChangesProcessorTest#getCountryWithAnnotationsCacheConfig()
     * 3. Wait 1 second until message with cache info will be send to GMC.
     * 4. Verify that cache info list contains cache with “CountryWithAnnotations” name.
     * 5. Wait 1 second until message with cache sql metadata list will be send to GMC.
     * 6. Verify that cache sql metadata list contains metadata for “CountryWithAnnotations” cache.
     * 7. Verify that cache sql metadata contains fields: ID, NAME, POPULATION with appropriate types (java.lang.Integer, java.lang.String, java.lang.Integer)
     */
    @Test
    public void shouldSendSqlMetadataForCacheCreatedByCacheConfigurationWithAnnotations() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.getOrCreateCache(getCountryWithAnnotationsCacheConfig());

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().anyMatch(i -> "CountryWithAnnotations".equals(i.getCacheName()));
        });

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "CountryWithAnnotations".equals(m.getCacheName()))
                .findFirst()
                .get();

            return cacheMeta != null &&
                "java.lang.Integer".equals(cacheMeta.getFields().get("ID")) &&
                "java.lang.String".equals(cacheMeta.getFields().get("NAME")) &&
                "java.lang.Integer".equals(cacheMeta.getFields().get("POPULATION")) &&
                cacheMeta.getIndexes().size() == 1;
        });
    }

    /**
     * GG-26556 Testcase 10:
     *
     * 1. Start 2 ignite nodes.
     * 2. Create a REPLICATED cache on second node.
     * 3. Verify the message with cache info is sent to GMC in less than 1 second.
     * 4. Verify that cache info list contains the cache created.
     * 5. Destroy the cache.
     * 6. Wait for 1 second until message with cache info will be sent to GMC.
     * 7. Verify that cache info list does not contains the cache anymore.
     */
    @Test
    public void shouldSendCacheInfoIfReplicatedCacheCreatedOnOtherNode() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        IgniteEx ignite_2 = startGrid(0);

        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>("test-cache-1").setCacheMode(REPLICATED);
        IgniteCache<Object, Object> cache = ignite_2.getOrCreateCache(cacheCfg);

        cache.put(1, 2);

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().anyMatch(i -> "test-cache-1".equals(i.getCacheName()));
        });

        cache.destroy();

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().noneMatch(i -> "test-cache-1".equals(i.getCacheName()));
        });
    }

    /**
     * GG-26556 Testcase 11:
     *
     * 1. Start 1 ignite node.
     * 2. Execute SQL query: "CREATE TABLE mc_agent_test_table_1 (id int, value int, PRIMARY KEY (id)) WITH "template=replicated";"
     * 3. Wait 1 second until message with cache info will be send to GMC.
     * 4. Verify that cache info list contains cache with “SQL_PUBLIC_MC_AGENT_TEST_TABLE_1” name.
     * 5. Execute SQL query: "DROP TABLE mc_agent_test_table_1;"
     * 6. Wait for 1 second until message with cache info will be sent to GMC.
     * 7. Verify that cache info list does not contain cache with “SQL_PUBLIC_MC_AGENT_TEST_TABLE_1” name.
     */
    @Test
    public void shouldSendCacheInfoOnCreatedOrDestroyedReplicatedCacheFromSql() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE mc_agent_test_table_1 (id int, value int, PRIMARY KEY (id)) WITH \"template=replicated\";"),
            true
        );

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().anyMatch(i -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_1".equals(i.getCacheName()));
        });

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("DROP TABLE mc_agent_test_table_1;"),
            true
        );

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().noneMatch(i -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_1".equals(i.getCacheName()));
        });
    }

    /**
     * @return Country cache configuration.
     */
    private CacheConfiguration<Integer, Country> getCountryCacheConfig() throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration("Country");

        ccfg.setSqlSchema("Country");

        Collection<QueryEntity> qryEntities = new ArrayList<>();

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setValueType(Country.class.getName());

        qryEntities.add(qryEntity);

        // Query fields for COUNTRY.
        LinkedHashMap<String, String> qryFlds = new LinkedHashMap<>();

        qryFlds.put("id", "java.lang.Integer");
        qryFlds.put("name", "java.lang.String");
        qryFlds.put("population", "java.lang.Integer");

        qryEntity.setFields(qryFlds);

        ccfg.setQueryEntities(qryEntities);

        return ccfg;
    }

    /**
     * @return Country with annotation cache configuration.
     */
    private CacheConfiguration<Integer, CountryWithAnnotations> getCountryWithAnnotationsCacheConfig() throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration("CountryWithAnnotations");

        ccfg.setSqlSchema("CountryWithAnnotations");

        ccfg.setIndexedTypes(Integer.class, CountryWithAnnotations.class);

        return ccfg;
    }
}
