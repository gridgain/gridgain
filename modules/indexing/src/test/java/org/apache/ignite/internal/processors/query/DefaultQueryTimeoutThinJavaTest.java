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

package org.apache.ignite.internal.processors.query;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.thin.ClientServerError;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;

public class DefaultQueryTimeoutThinJavaTest extends DefaultQueryTimeoutTest {
    @Override protected void executeQuery(String sql) throws Exception {
        try (IgniteClient cli = G.startClient(new ClientConfiguration().setAddresses("127.0.0.1"))) {
            cli.query(new SqlFieldsQuery(sql)).getAll();
        }
    }

    @Override protected void executeQuery(String sql, long timeout) throws Exception {
        try (IgniteClient cli = G.startClient(new ClientConfiguration().setAddresses("127.0.0.1"))) {
            cli.query(new SqlFieldsQuery(sql).setTimeout((int)timeout, TimeUnit.MILLISECONDS)).getAll();
        }
    }

    @Override protected void assertQueryCancelled(Callable<?> c) {
        try {
            c.call();
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, "The query was cancelled while executing", ClientServerError.class));
        }
    }
}
