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
package org.apache.ignite.internal.client;

import java.util.List;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.jdbc.thin.JdbcThinDiskSpillingClientsTest;
import org.junit.Ignore;

/**
 * Offloading test for Thin clients.
 */
@Ignore("https://ggsystems.atlassian.net/browse/GG-26457")
public class ThinClientDiskSpillingClientsTest extends JdbcThinDiskSpillingClientsTest {
    /** {@inheritDoc} */
    @Override protected ClientQueryRunner createQueryRunner() {
        return new ThinClientQueryRunner();
    }

    /**
     * Thin client query runner.
     */
    static class ThinClientQueryRunner extends ClientQueryRunner {
        private IgniteClient cli;

        @Override protected void initInternal() {
            cli = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"));
        }

        @Override protected List<List<?>> runQuery(String qry) {
            return cli.query(new SqlFieldsQueryEx(qry, null)
                .setMaxMemory(SMALL_MEM_LIMIT))
                .getAll();
        }
    }
}
