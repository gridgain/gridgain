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

package org.apache.ignite.yardstick.jdbc;

import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs SQL MERGE operations.
 */
public class ThinJdbcSqlMergeDateTypeBenchmark extends AbstractJdbcBenchmark {
    /** {@inheritDoc} */
    @Override protected void setupData() throws Exception {
        String dataTypes = args.getStringParameter("types", "DATE");

        System.out.println("+++ dataTypes=" + dataTypes);

        try (Statement stmt =  conn.get().createStatement()) {
            stmt.execute("");
        }
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        return true;
    }
}
