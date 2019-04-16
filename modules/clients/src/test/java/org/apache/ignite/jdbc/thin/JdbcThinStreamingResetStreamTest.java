/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test JDBC streaming with restart / reset multiple times.
 */
public class JdbcThinStreamingResetStreamTest extends GridCommonAbstractTest {
    /** JDBC URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** JDBC Connection. */
    private Connection conn;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        conn = DriverManager.getConnection(URL, new Properties());

        conn.prepareStatement("CREATE TABLE test(id LONG PRIMARY KEY, val0 VARCHAR, val1 VARCHAR) " +
            "WITH \"template=replicated\"").execute();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            conn.prepareStatement("SET STREAMING OFF").execute();
            conn.prepareStatement("DROP TABLE test").execute();
        }
        finally {
            U.close(conn, log);
        }

        super.afterTest();
    }

    /**
     * @throws Exception On fails.
     */
    @Test
    public void testOrdered() throws Exception {
        checkStreamReset(true);
    }

    /**
     * @throws Exception On fails.
     */
    @Test
    public void testNotOrdered() throws Exception {
        checkStreamReset(false);
    }

    /**
     * @throws Exception On fails.
     */
    @Test
    public void testOrderedResetWorkerCreationRace() throws Exception {
        final long BATCH_SIZE = 2;
        final long ITERATIONS = 1000;

        for (int iter = 0; iter < ITERATIONS; ++iter) {
            conn.prepareStatement("SET STREAMING ON BATCH_SIZE " + BATCH_SIZE + " ORDERED").execute();

            String sql = "INSERT INTO test (id, val0, val1) VALUES (?, ?, ?)";

            PreparedStatement ps = conn.prepareStatement(sql);

            ps.setInt(1, (int)Math.round(Math.random()));
            ps.setString(2, String.valueOf(Math.random()));
            ps.setString(3, String.valueOf(Math.random()));

            ps.execute();
        }
    }

    /**
     * @param ordered Use ordered Stream.
     * @throws Exception On fails.
     */
    public void checkStreamReset(boolean ordered) throws Exception {
        final long BATCH_SIZE = 4096;
        final long ROWS = BATCH_SIZE * 2 + 1;
        final long ITERATIONS = 100;

        for (int iter = 0; iter < ITERATIONS; ++iter) {
            conn.prepareStatement("SET STREAMING ON FLUSH_FREQUENCY 1000 BATCH_SIZE " + BATCH_SIZE
                + (ordered ? " ORDERED" : "")).execute();

            String sql = "INSERT INTO test (id, val0, val1) VALUES (?, ?, ?)";

            PreparedStatement ps = conn.prepareStatement(sql);

            for (int i = 0; i < ROWS; i++) {
                ps.setInt(1, i);
                ps.setString(2, String.valueOf(Math.random()));
                ps.setString(3, String.valueOf(Math.random()));

                ps.execute();
            }

            conn.prepareStatement("SET STREAMING OFF").execute();
        }
    }
}
