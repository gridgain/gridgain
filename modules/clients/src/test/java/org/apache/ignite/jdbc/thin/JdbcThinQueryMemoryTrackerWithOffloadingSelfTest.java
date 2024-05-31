/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

import static org.apache.ignite.internal.util.IgniteUtils.KB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.internal.processors.query.oom.AbstractQueryMemoryTrackerSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Offloading enabled tests. */
@RunWith(Parameterized.class)
public class JdbcThinQueryMemoryTrackerWithOffloadingSelfTest extends AbstractQueryMemoryTrackerSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
                .setSqlConfiguration(new SqlConfiguration()
                        .setSqlOffloadingEnabled(true)
                        .setSqlGlobalMemoryQuota(Long.toString(globalQuotaSize())));
    }

    /** Lazy flag params. */
    @Parameterized.Parameters(name = "lazy = {0}")
    public static Iterable<Object[]> valuesForAsync() {
        return Arrays.asList(new Object[][] {
                {true},
                {false}
        });
    }

    /** Lazy flag. */
    @Parameterized.Parameter
    public boolean lazy;

    /** Check memory release correctness . */
    @Test
    public void testCorrectCloseMemoryTrackerWithOffloading() throws Throwable {
        maxMem = 100 * KB;

        LogListener logLsnr = LogListener
                .matches("Offloading started for query")
                .build();

        testLog(grid(0)).registerListener(logLsnr);

        try (Connection conn = createConnection(lazy)) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT TOP 1 * FROM (SELECT T.id, T.ref_key, T.name FROM T inner join "
                        + "T k2 ON T.ref_key=k2.ref_key where T.id > 0 group by T.id) X");

                assertTrue(rs.next());
            }
        }

        assertTrue(logLsnr.check());
    }

    /** */
    private ListeningTestLogger testLog(IgniteEx node) {
        ListeningTestLogger testLog = new ListeningTestLogger(log);

        GridTestUtils.setFieldValue(memoryManager(node), "log", testLog);

        return testLog;
    }

    /** */
    private QueryMemoryManager memoryManager(IgniteEx node) {
        IgniteH2Indexing h2 = (IgniteH2Indexing)node.context().query().getIndexing();

        return h2.memoryManager();
    }

    /** {@inheritDoc} */
    @Override
    protected boolean isLocal() {
        return false;
    }

    /** */
    protected Connection createConnection(boolean lazy) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800..10802?" +
                "queryMaxMemory=" + (maxMem) + "&lazy=" + lazy);

        conn.setSchema("\"PUBLIC\"");

        return conn;
    }
}
