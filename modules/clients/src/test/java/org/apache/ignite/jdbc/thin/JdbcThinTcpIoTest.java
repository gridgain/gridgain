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

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl;
import org.apache.ignite.internal.jdbc.thin.JdbcThinTcpIo;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for JdbcThinTcpIo.
 */
public class JdbcThinTcpIoTest extends GridCommonAbstractTest {
    /**
     * Test connection to host with accessible address.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testHostWithValidAddress() throws Exception {
        startGrids(1);

        JdbcThinTcpIo jdbcThinTcpIo = null;

        try {
            jdbcThinTcpIo = new JdbcThinTcpIo(new ConnectionPropertiesImpl(),
                new InetSocketAddress("127.0.0.1", 10800), null, 500);
        }
        finally {
            if (jdbcThinTcpIo != null)
                jdbcThinTcpIo.close();
        }

        stopGrid(0);
    }

    /**
     * Test exception text (should contain inaccessible ip addresses list).
     */
    @Test
    public void testExceptionMessage() {
        Throwable throwable = GridTestUtils.assertThrows(log, new Callable<Object>() {
            @SuppressWarnings("ResultOfObjectAllocationIgnored")
            @Override public Object call() throws Exception {
                new JdbcThinTcpIo(new ConnectionPropertiesImpl(),
                    new InetSocketAddress("10.0.0.0", 10800), null, 500);

                return null;
            }
        }, SQLException.class, "Failed to connect to server [host=/10.0.0.0, port=10800]");

        assertEquals(java.net.SocketTimeoutException.class, throwable.getCause().getClass());

        assertTrue(throwable.getCause().getMessage().contains("connect timed out"));
    }
}
