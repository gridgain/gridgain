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

package org.apache.ignite.cache.hibernate;

import java.util.Collections;
import javax.cache.configuration.Factory;
import javax.transaction.Synchronization;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import org.apache.commons.dbcp.managed.BasicManagedDataSource;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.gridgain.internal.h2.jdbcx.JdbcDataSource;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.engine.transaction.internal.jta.JtaTransactionFactory;
import org.hibernate.engine.transaction.spi.TransactionFactory;
import org.hibernate.service.ServiceRegistryBuilder;
import org.hibernate.service.jdbc.connections.internal.DatasourceConnectionProviderImpl;
import org.hibernate.service.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.jta.platform.internal.AbstractJtaPlatform;
import org.hibernate.service.jta.platform.spi.JtaPlatform;
import org.jetbrains.annotations.Nullable;
import org.objectweb.jotm.Jotm;

/**
 *
 * Tests Hibernate L2 cache with TRANSACTIONAL access mode (Hibernate and Cache are configured
 * to used the same TransactionManager).
 */
public class HibernateL2CacheTransactionalSelfTest extends HibernateL2CacheSelfTest {
    /** */
    private static Jotm jotm;

    /**
     */
    private static class TestJtaPlatform extends AbstractJtaPlatform {
        /** {@inheritDoc} */
        @Override protected TransactionManager locateTransactionManager() {
            return jotm.getTransactionManager();
        }

        /** {@inheritDoc} */
        @Override protected UserTransaction locateUserTransaction() {
            return jotm.getUserTransaction();
        }
    }

    /**
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestTmFactory implements Factory<TransactionManager> {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public TransactionManager create() {
            return jotm.getTransactionManager();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        jotm = new Jotm(true, false);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (jotm != null)
            jotm.stop();

        jotm = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxManagerFactory(new TestTmFactory());
        cfg.getTransactionConfiguration().setUseJtaSynchronization(useJtaSynchronization());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration transactionalRegionConfiguration(String regionName) {
        CacheConfiguration cfg = super.transactionalRegionConfiguration(regionName);

        cfg.setNearConfiguration(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected ServiceRegistryBuilder registryBuilder() {
        ServiceRegistryBuilder builder = new ServiceRegistryBuilder();

        DatasourceConnectionProviderImpl connProvider = new DatasourceConnectionProviderImpl();

        BasicManagedDataSource dataSrc = new BasicManagedDataSource(); // JTA-aware data source.

        dataSrc.setTransactionManager(jotm.getTransactionManager());

        dataSrc.setDefaultAutoCommit(false);

        JdbcDataSource h2DataSrc = new JdbcDataSource();

        h2DataSrc.setURL(CONNECTION_URL);

        dataSrc.setXaDataSourceInstance(h2DataSrc);

        connProvider.setDataSource(dataSrc);

        connProvider.configure(Collections.emptyMap());

        builder.addService(ConnectionProvider.class, connProvider);

        builder.addService(JtaPlatform.class, new TestJtaPlatform());

        builder.addService(TransactionFactory.class, new JtaTransactionFactory());

        return builder;
    }

    /** {@inheritDoc} */
    @Override protected AccessType[] accessTypes() {
        return new AccessType[]{AccessType.TRANSACTIONAL};
    }

    /**
     * @return Whether to use {@link Synchronization}.
     */
    protected boolean useJtaSynchronization() {
        return false;
    }
}
