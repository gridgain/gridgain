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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.jdbc2.JdbcQueryMemoryTrackerSelfTest;
import org.apache.ignite.internal.processors.query.h2.H2LocalResultFactory;
import org.apache.ignite.internal.processors.query.h2.H2ManagedLocalResult;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.expression.Expression;
import org.gridgain.internal.h2.result.LocalResult;
import org.gridgain.internal.h2.result.LocalResultImpl;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static org.apache.ignite.internal.util.IgniteUtils.MB;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Query memory manager for local queries.
 */
public class JdbcThinQueryMemoryTrackerSelfTest extends JdbcQueryMemoryTrackerSelfTest {
    /** Hold any error that occurred while closing the local result. */
    private static final AtomicReference<Throwable> localResultErr = new AtomicReference<>();

    /** Count of closed results. */
    private static final AtomicInteger closedResultCntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected Class<?> localResultFactory() {
        return TestH2LocalResultFactory.class;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        closedResultCntr.set(0);
    }

    /**
     * Ensures that the H2 local result closes without errors when only a small part of the results are read.
     *
     * @throws Throwable If failed.
     */
    @Test
    public void testPartialResultRead() throws Throwable {
        maxMem = 8 * MB;

        try (Connection conn = createConnection(false)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select * from k")) {
                    for (int i = 0; i < 10; i++) {
                        if (!rs.next())
                            fail();
                    }
                }
            }
        }

        // A wait is required because the query cancel request handler runs asynchronously.
        boolean success = waitForCondition(() -> localResults.size() == closedResultCntr.get(), getTestTimeout());
        assertTrue(success);

        Throwable err = localResultErr.get();

        if (err != null) {
            fail(err.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override protected Connection createConnection(boolean lazy) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800..10802?" +
            "queryMaxMemory=" + (maxMem) + "&lazy=" + lazy);

        conn.setSchema("\"PUBLIC\"");

        return conn;
    }

    /**
     * Local result factory for test.
     */
    public static class TestH2LocalResultFactory extends H2LocalResultFactory {
        /** {@inheritDoc} */
        @Override public LocalResult create(Session ses, Expression[] expressions, int visibleColCnt, boolean system) {
            if (system)
                return new LocalResultImpl(ses, expressions, visibleColCnt);

            if (ses.memoryTracker() != null) {
                H2ManagedLocalResult res = new H2ManagedLocalResult(ses, expressions, visibleColCnt) {
                    @Override public void close() {
                        try {
                            closedResultCntr.incrementAndGet();

                            super.close();
                        } catch (Throwable t) {
                            if (!localResultErr.compareAndSet(null, t))
                                localResultErr.get().addSuppressed(t);

                            throw t;
                        }
                    }
                };

                localResults.add(res);

                return res;
            }

            return new H2ManagedLocalResult(ses, expressions, visibleColCnt);
        }

        /** {@inheritDoc} */
        @Override public LocalResult create() {
            throw new NotImplementedException();
        }
    }
}
