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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.jdbc2.JdbcQueryMemoryTrackerSelfTest;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryTracker;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.MB;

/**
 * Query memory manager for local queries.
 */
public class JdbcThinQueryMemoryTrackerSelfTest extends JdbcQueryMemoryTrackerSelfTest {
    /** Log listener. */
    private static final LogListener errMsgLsnr =
        LogListener.matches(QueryMemoryTracker.ERROR_TRACKER_ALREADY_CLOSED).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        ListeningTestLogger lnsrLog = new ListeningTestLogger(log());

        lnsrLog.registerListener(errMsgLsnr);

        return super.getConfiguration(igniteInstanceName).setGridLogger(lnsrLog);
    }

    /**
     * Ensures that the memory tracker closes without errors when only a small part of the results are read.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartialResultRead() throws Exception {
        maxMem = 8 * MB;

        System.out.println(">xxx> start test");

        try (Connection conn = createConnection(false)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select * from k")) {
                    for (int i = 0; i < 10; i++) {
                        if (!rs.next())
                            return;
                    }
                }
            }
        }

        assertFalse(errMsgLsnr.check(1_000));
    }

    /** {@inheritDoc} */
    @Override protected Connection createConnection(boolean lazy) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800..10802?" +
            "queryMaxMemory=" + (maxMem) + "&lazy=" + lazy);

        conn.setSchema("\"PUBLIC\"");

        return conn;
    }
}