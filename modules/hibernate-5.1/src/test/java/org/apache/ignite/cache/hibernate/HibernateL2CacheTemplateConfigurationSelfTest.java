/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.cache.hibernate;

import javax.persistence.Cacheable;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.entity.NotCachedEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.hibernate.HibernateAccessStrategyFactory.REGION_CACHE_PROPERTY;
import static org.apache.ignite.cache.hibernate.HibernateL2CacheConfigurationSelfTest.QUERY_CACHE;
import static org.apache.ignite.cache.hibernate.HibernateL2CacheConfigurationSelfTest.TIMESTAMP_CACHE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.hibernate.cfg.AvailableSettings.CACHE_REGION_FACTORY;
import static org.hibernate.cfg.AvailableSettings.GENERATE_STATISTICS;
import static org.hibernate.cfg.AvailableSettings.HBM2DDL_AUTO;
import static org.hibernate.cfg.AvailableSettings.RELEASE_CONNECTIONS;
import static org.hibernate.cfg.AvailableSettings.USE_QUERY_CACHE;
import static org.hibernate.cfg.AvailableSettings.USE_SECOND_LEVEL_CACHE;

/**
 * Tests Hibernate L2 cache template configuration.
 */
public class HibernateL2CacheTemplateConfigurationSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String ENTITY1_NAME = Entity1.class.getName();

    /** */
    public static final String NOT_CACHED_ENTITY_NAME = NotCachedEntity.class.getName();

    /** */
    public static final String CONNECTION_URL = "jdbc:gg-h2:mem:example;DB_CLOSE_DELAY=-1";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        for (IgniteCacheProxy<?, ?> cache : ((IgniteKernal)grid(0)).caches())
            cache.clear();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi)
            .setCacheConfiguration(
                cacheConfiguration(TIMESTAMP_CACHE),
                cacheConfiguration(QUERY_CACHE),
                cacheConfiguration("org.apache.ignite.cache.hibernate.*")
            );

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<?, ?> cacheConfiguration(String cacheName) {
        return new CacheConfiguration<>()
            .setName(cacheName)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(ATOMIC)
            .setStatisticsEnabled(true);
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Hibernate configuration.
     */
    private Configuration hibernateConfiguration(String igniteInstanceName) {
        return new Configuration()
            .addAnnotatedClass(Entity1.class)
            .addAnnotatedClass(NotCachedEntity.class)
            .setProperty(HibernateAccessStrategyFactory.DFLT_ACCESS_TYPE_PROPERTY, AccessType.NONSTRICT_READ_WRITE.name())
            .setProperty(HBM2DDL_AUTO, "create")
            .setProperty(GENERATE_STATISTICS, "true")
            .setProperty(USE_SECOND_LEVEL_CACHE, "true")
            .setProperty(USE_QUERY_CACHE, "true")
            .setProperty(CACHE_REGION_FACTORY, HibernateRegionFactory.class.getName())
            .setProperty(RELEASE_CONNECTIONS, "on_close")
            .setProperty(HibernateAccessStrategyFactory.IGNITE_INSTANCE_NAME_PROPERTY, igniteInstanceName)
            .setProperty(REGION_CACHE_PROPERTY + TIMESTAMP_CACHE, TIMESTAMP_CACHE)
            .setProperty(REGION_CACHE_PROPERTY + QUERY_CACHE, QUERY_CACHE);
    }

    /**
     * Tests ignite template cache configuration for Hibernate L2 cache.
     * Description:
     * 1. Try to save two @{@link Cacheable} entities into the database, one of them ({@link Entity1}) has an L2 cache
     * created from the template and the other one ({@link NotCachedEntity}) doesn't.
     * 2. Get {@link IllegalArgumentException} as {@link NotCachedEntity} doesn't have an Ignite cache configured.
     * 3. Try to save only {@link Entity1}.
     * 4. Test that {@link Entity1} has been successfully inserted into L2 cache twice.
     */
    @Test
    public void testTemplateCacheConfiguration() {
        try (SessionFactory sesFactory = startHibernate(getTestIgniteInstanceName(0))) {
            String expectedMsg = "Cache '" + NOT_CACHED_ENTITY_NAME + "' for region '" + NOT_CACHED_ENTITY_NAME
                + "' is not configured.";
            // Although caching of NotCachedEntity fails, Entity1 should be cached successfully
            assertThrowsAnyCause(log, () -> {
                try (Session ses = sesFactory.openSession()) {
                    Transaction tx = ses.beginTransaction();

                    // Can be put into the L2 cache
                    ses.save(new Entity1());

                    // Can't be put into the L2 cache
                    ses.save(new NotCachedEntity());

                    // Fails as one of the entities marked Cacheable can't be put into L2 cache
                    tx.commit();
                }
                return null;
            }, IllegalArgumentException.class, expectedMsg);

            try (Session ses = sesFactory.openSession()) {
                Transaction tx = ses.beginTransaction();

                // Can be put into the L2 cache
                ses.save(new Entity1());

                tx.commit();
            }

            IgniteCache<Object, Object> cache1 = grid(0).cache(ENTITY1_NAME);

            assertEquals(2, cache1.size());

            IgniteCache<Object, Object> cache = grid(0).cache(NOT_CACHED_ENTITY_NAME);

            assertNull(cache);
        }
    }

    /**
     * @param igniteInstanceName Name of the grid providing caches.
     * @return Session factory.
     */
    private SessionFactory startHibernate(String igniteInstanceName) {
        Configuration cfg = hibernateConfiguration(igniteInstanceName);

        StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder();

        builder.applySetting("hibernate.connection.url", CONNECTION_URL);
        builder.applySetting("hibernate.show_sql", false);
        builder.applySettings(cfg.getProperties());

        return cfg.buildSessionFactory(builder.build());
    }

    /**
     * Test Hibernate entity1.
     */
    @javax.persistence.Entity
    @SuppressWarnings({"PublicInnerClass", "UnnecessaryFullyQualifiedName"})
    @Cacheable
    @org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
    public static class Entity1 {
        /** */
        private int id;

        /**
         * @return ID.
         */
        @Id
        @GeneratedValue
        public int getId() {
            return id;
        }

        /**
         * @param id ID.
         */
        public void setId(int id) {
            this.id = id;
        }
    }
}
