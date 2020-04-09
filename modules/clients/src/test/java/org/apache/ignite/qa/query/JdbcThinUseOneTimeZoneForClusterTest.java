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

package org.apache.ignite.qa.query;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.UseOneTimeZoneForClusterTest;
import org.apache.ignite.jdbc.JdbcTestUtils;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.Test;

/**
 */
public class JdbcThinUseOneTimeZoneForClusterTest extends UseOneTimeZoneForClusterTest {
    /** Jdbc thin url base. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** Jdbc thin url. */
    private static String url = URL;

    /** {@inheritDoc} */
    @Override protected List<List<?>> sql(String sql, List<Object> params) throws Exception {
        return JdbcTestUtils.sql(url, sql, params);
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testClientsInDifferentTimeZones() throws Exception {
        super.testClientsInDifferentTimeZones();

        IgniteEx ign = grid(INIT_NODE_NAME);

        Collection<Integer> thinPorts = ign.compute(ign.cluster().forClients()).broadcast(new IgniteCallable<Integer>() {
            @IgniteInstanceResource
            Ignite ign;

            @Override public Integer call() {
                return ((IgniteEx)ign).context().sqlListener().port();
            }
        });

        for (int port : thinPorts) {
            url = URL + ":" + port;

            checkDates();
        }
    }
}
