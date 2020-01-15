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
package org.apache.ignite.jdbc.thin;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingAbstractTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;

/**
 * Spilling test for thin JDBC.
 */
public class JdbcThinDiskSpillingClientsTest extends DiskSpillingAbstractTest {
    /** */
    private ClientQueryRunner qryRunner;

    /**
     * @return New query runner.
     */
    protected ClientQueryRunner createQueryRunner() {
        return new JdbcThinClient();
    }

    /**
     *
     */
    @Test
    public void testSimpleSelect() {
        getQueryRunner().checkQuery("SELECT * FROM person");
    }

    /**
     *
     */
    @Test
    public void testSelectSorted() {
        getQueryRunner().checkQuery("SELECT code FROM person ORDER BY code");
    }

    /**
     *
     */
    @Test
    public void testSelectDistinct() {
        getQueryRunner().checkQuery("SELECT DISTINCT depId, code FROM person ORDER BY depId, code");
    }

    /**
     *
     */
    @Test
    public void testSelectAggGroupBy() {
        getQueryRunner().checkQuery("SELECT code, age, AVG(DISTINCT temperature), SUM(DISTINCT temperature), " +
            "MAX(DISTINCT temperature), MIN(DISTINCT temperature) FROM person GROUP BY age, code");
    }

    /**
     *
     */
    @Test
    public void testSelectDML() {
        getQueryRunner().checkQuery("UPDATE person " +
            "SET age = age + 1 " +
            "WHERE age >= 0");
    }

    /**
     *
     */
    @Test
    public void testSelectIntersect() {
        getQueryRunner().checkQuery("(" +
            "(SELECT * FROM person WHERE depId < 20 ORDER BY depId) " +
            "INTERSECT SELECT * FROM person WHERE depId > 10 ) " +
            "UNION ALL SELECT DISTINCT * FROM person WHERE age <100 " +
            "ORDER BY id LIMIT 10000 OFFSET 20");
    }

    /**
     * Inits query runner.
     */
    private ClientQueryRunner getQueryRunner() {
        if (qryRunner == null) {
            qryRunner = createQueryRunner();

            qryRunner.init(grid(0), getWorkDir());
        }

        return qryRunner;
    }

    /**
     * Abstract base for running query from different clients.
     */
    protected abstract static class ClientQueryRunner {
        /** */
        protected IgniteEx server;

        /** */
        protected Path workDir;

        /**
         * @param srv Server.
         * @param workDir Work dir.
         */
        void init(IgniteEx srv, Path workDir) {
            this.server = srv;
            this.workDir = workDir;

            initInternal();
        }

        /**
         * @param qry Checks query correctness when offloading is enabled.
         */
        void checkQuery(String qry) {
            WatchService watchSvc = null;

            try {
                // Check offload happened.
                watchSvc = FileSystems.getDefault().newWatchService();

                WatchKey watchKey = workDir.register(watchSvc, ENTRY_CREATE, ENTRY_DELETE);

                List<List<?>> res = runQuery(qry);

                List<WatchEvent<?>> dirEvts = watchKey.pollEvents();

                // Check files have been created but deleted later.
                assertFalse("Disk spilling not happened.", dirEvts.isEmpty());

                // Assert workdir is empty.
                assertEquals(0, workDir.toFile().list().length);

                // Check for result correctness.
                List<List<?>> intApiRes = server.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(qry)).getAll();

                assertEqualsCollections(intApiRes, res);
            }
            catch (Exception e) {
                throw new AssertionError(e);
            }
            finally {
                U.closeQuiet(watchSvc);
            }
        }

        /**
         * Inits client.
         */
        protected abstract void initInternal();

        /**
         * Runs query.
         *
         * @param qry Query.
         * @return Result.
         */
        protected abstract List<List<?>> runQuery(String qry);

        /** {@inheritDoc} */
        @Override public String toString() {
            return getClass().getSimpleName();
        }
    }

    /**
     *
     */
    protected static class JdbcThinClient extends ClientQueryRunner {
        /** */
        private Connection conn;

        /** {@inheritDoc} */
        @Override protected void initInternal() {
            try {
                assertNotNull(server);

                conn = DriverManager.getConnection(getConnectionString());

                conn.setSchema("\"PUBLIC\"");
            }
            catch (SQLException e) {
                throw new AssertionError(e);
            }
        }

        /**
         * @return Connection string
         */
        protected String getConnectionString() {
            return "jdbc:ignite:thin://127.0.0.1:10800..10802?" +
                "queryMaxMemory=" + (SMALL_MEM_LIMIT);
        }

        /** {@inheritDoc} */
        @Override protected List<List<?>> runQuery(String qry) {
            try {
                assertNotNull(conn);

                try (PreparedStatement s = conn.prepareStatement(qry)) {
                    if (s.execute()) {
                        List<List<?>> res = new ArrayList<>();

                        try (ResultSet rs = s.getResultSet()) {
                            ResultSetMetaData meta = rs.getMetaData();

                            int cnt = meta.getColumnCount();

                            while (rs.next()) {
                                List<Object> row = new ArrayList<>(cnt);

                                for (int i = 1; i <= cnt; i++)
                                    row.add(rs.getObject(i));

                                res.add(row);
                            }
                        }

                        return res;
                    }
                    else // DML
                        return Collections.singletonList(Collections.singletonList((long)s.getUpdateCount()));
                }

            }
            catch (SQLException e) {
                e.printStackTrace();

                throw new AssertionError(e);
            }
        }
    }
}
