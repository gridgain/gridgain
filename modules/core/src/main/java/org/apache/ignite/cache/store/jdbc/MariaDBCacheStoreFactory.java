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

package org.apache.ignite.cache.store.jdbc;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.jdbc.dialect.MariaDBDialect;
import org.mariadb.jdbc.MariaDbDataSource;

import javax.cache.configuration.Factory;
import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * {@link Factory} implementation for {@link MariaDBCacheStore}. Use this factory to pass a
 * MariaDB-backed {@link MariaDBCacheStore} to a {@code CacheConfiguration}. For
 * dialect-agnostic configuration, {@link CacheJdbcPojoStoreFactory} combined with
 * {@code setDialect(new MariaDBDialect())} is the standard alternative.
 */
public class MariaDBCacheStoreFactory<K, V> implements Factory<MariaDBCacheStore<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Maximum batch size for writeAll and deleteAll operations. */
    private int batchSize = 512;

    /** Maximum worker thread count. These threads are responsible for load-cache operations. */
    private int maxPoolSize = Runtime.getRuntime().availableProcessors();

    /** Parallel load-cache minimum threshold. If {@code 0} then load sequentially. */
    private int parallelLoadCacheMinThreshold = 512;

    /** JDBC types this store can process. */
    private JdbcType[] types;

    /** Data source factory. */
    private Factory<DataSource> dataSrcFactory;

    /** Flag indicating that table and field names should be escaped in all SQL queries created by the JDBC POJO store. */
    private boolean sqlEscapeAll;

    /** Dialect fetch size. See {@link MariaDBDialect#MariaDBDialect()}.*/
    private int fetchSize = 1;


    /**
     * Builds and returns a fully configured {@link MariaDBCacheStore}.
     *
     * @return Configured {@link MariaDBCacheStore} ready to be plugged into a {@code CacheConfiguration}.
     * @throws IgniteException If no data source factory has been configured via {@link #setDataSourceFactory}.
     */
    @Override public MariaDBCacheStore<K, V> create() {
        MariaDBCacheStore<K, V> store = new MariaDBCacheStore<>();

        store.setBatchSize(batchSize);
        store.setDialect(new MariaDBDialect(fetchSize));
        store.setMaximumPoolSize(maxPoolSize);
        store.setParallelLoadCacheMinimumThreshold(parallelLoadCacheMinThreshold);
        store.setTypes(types);
        store.setHasher(JdbcTypeDefaultHasher.INSTANCE);
        store.setTransformer(JdbcTypesDefaultTransformer.INSTANCE);
        store.setSqlEscapeAll(sqlEscapeAll);

        if (dataSrcFactory != null) {
            store.setDataSource(dataSrcFactory.create());
        } else {
            throw new IgniteException("No MariaDB data source configured");
        }

        return store;
    }

    /**
     * Sets the maximum batch size for write and delete operations.
     *
     * @param batchSize Maximum batch size.
     * @return {@code This} for chaining.
     */
    public MariaDBCacheStoreFactory<K, V> setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Sets the maximum worker thread count used for load-cache operations.
     *
     * @param maxPoolSize Maximum worker thread count.
     * @return {@code This} for chaining.
     */
    public MariaDBCacheStoreFactory<K, V> setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    /**
     * Sets the parallel load-cache minimum threshold. If {@code 0} then load sequentially.
     *
     * @param threshold Parallel load-cache minimum threshold.
     * @return {@code This} for chaining.
     */
    public MariaDBCacheStoreFactory<K, V> setParallelLoadCacheMinimumThreshold(int threshold) {
        this.parallelLoadCacheMinThreshold = threshold;
        return this;
    }

    /**
     * Sets the JDBC types this store can process.
     *
     * @param types JDBC type mappings.
     * @return {@code This} for chaining.
     */
    public MariaDBCacheStoreFactory<K, V> setTypes(JdbcType... types) {
        this.types = types;
        return this;
    }

    /**
     * Sets the data source factory. The factory is invoked when the store is created and must
     * survive serialization since Ignite distributes cache configuration across nodes.
     *
     * @param dataSrcFactory Data source factory.
     * @return {@code This} for chaining.
     */
    public MariaDBCacheStoreFactory<K, V> setDataSourceFactory(Factory<DataSource> dataSrcFactory) {
        this.dataSrcFactory = dataSrcFactory;
        return this;
    }

    /**
     * Convenience setter that installs a built-in serializable factory that builds a
     * {@link MariaDbDataSource} from the supplied JDBC coordinates.
     *
     * @param url JDBC URL.
     * @param user JDBC user.
     * @param pwd JDBC password.
     * @return {@code This} for chaining.
     */
    public MariaDBCacheStoreFactory<K, V> setDataSourceFactory(String url, String user, String pwd) {
        return setDataSourceFactory(dataSrcFactory(url, user, pwd));
    }

    /**
     * Flag indicating that table and field names should be escaped in all SQL queries created by the JDBC POJO store.
     *
     * @param sqlEscapeAll {@code true} to escape all table and field names.
     * @return {@code This} for chaining.
     */
    public MariaDBCacheStoreFactory<K, V> setSqlEscapeAll(boolean sqlEscapeAll) {
        this.sqlEscapeAll = sqlEscapeAll;
        return this;
    }

    /**
     * Creates a dialect with an explicit fetch size.
     * <p>
     * For streaming result sets, MariaDB Connector/J Before version 1.4.0, the only accepted value for fetch size
     * was {@code Statement.setFetchSize(Integer.MIN_VALUE)} (equivalent to {@code Statement.setFetchSize(1)}).
     * This value is still accepted for compatibility reasons, but rather use {@code Statement.setFetchSize(1)},
     * since according to JDBC the value must be >= 0.
     * <p>
     * See <a href="https://mariadb.com/docs/connectors/mariadb-connector-j/about-mariadb-connector-j#streaming-result-sets">MariaDB
     * Connector/J streaming result sets</a>.
     *
     * @param fetchSize Fetch size passed to JDBC {@code Statement.setFetchSize} for load-cache range queries.
     */
    public MariaDBCacheStoreFactory<K, V> setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * Default {@link Factory} that builds a {@link MariaDbDataSource}. Coordinates are passed to the constructor so
     * the factory survives serialization (Ignite distributes cache configuration across nodes).
     */
    private Factory<DataSource> dataSrcFactory(String url, String user, String pwd) {
        return () -> {
            try {
                MariaDbDataSource ds = new MariaDbDataSource();

                ds.setUrl(url);
                ds.setUser(user);
                ds.setPassword(pwd);

                return ds;
            } catch (SQLException e) {
                throw new IgniteException("Failed to build MariaDB DataSource for url=" + url, e);
            }
        };
    }
}
