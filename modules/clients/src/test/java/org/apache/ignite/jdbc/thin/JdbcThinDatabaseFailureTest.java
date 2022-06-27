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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.processors.query.h2.H2LocalResultFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.expression.Expression;
import org.gridgain.internal.h2.result.LocalResult;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/** Test triggering of failure handler cases. */
public class JdbcThinDatabaseFailureTest extends JdbcThinAbstractSelfTest {
    /** Emulate h2 db failure flag. */
    private static volatile boolean criticalDbFail;

    /** */
    private TestFailureHandler fHnd;

    /** If {@code true} wraps default local result factory. */
    private boolean fakeLocalFactory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        if (fakeLocalFactory)
            System.setProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY, TestH2LocalResultFactory.class.getName());

        return super.getConfiguration(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        fHnd = new TestFailureHandler(false);
        return fHnd;
    }

    /** Tests failure handler in case of h2 db is down by some reasons.*/
    @Test
    public void fhTriggersIfH2DbIsDown() throws Exception {
        fakeLocalFactory = true;
        startGrid(1);

        Connection conn = createConnection();
        Statement stmt = conn.createStatement();

        stmt.execute("create table t1 (k1 int primary key, k2 int)");

        String sql = "select * from t1";

        GridTestUtils.assertThrows(log, () -> {
            criticalDbFail = true;
            Statement stmt1 = conn.createStatement();
            stmt1.execute(sql);
            stmt1.getResultSet();
        }, IgniteException.class, "Critical database error");

        Throwable err = fHnd.failureContext().error();

        String msg = err.getMessage();

        assertTrue(msg.contains("The database has been closed"));
    }

    /** */
    protected Connection createConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
    }

    /** */
    public static class TestH2LocalResultFactory extends H2LocalResultFactory {
        /** {@inheritDoc} */
        @Override public LocalResult create(Session ses, Expression[] expressions, int visibleColCnt, boolean system) {
            if (criticalDbFail)
                ses.setPowerOffCount(1);

            return super.create(ses, expressions, visibleColCnt, system);
        }

        /** {@inheritDoc} */
        @Override public LocalResult create() {
            throw new NotImplementedException();
        }
    }
}
