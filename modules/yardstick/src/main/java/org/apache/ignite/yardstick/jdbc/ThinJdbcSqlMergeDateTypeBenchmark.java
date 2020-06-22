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

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite benchmark that performs SQL MERGE operations.
 */
public class ThinJdbcSqlMergeDateTypeBenchmark extends AbstractJdbcBenchmark {
    /** Types suppliers. */
    private Map<String, Function<Integer, Object>> valCreatorMap = new HashMap<>();

    {
        valCreatorMap.put("TINYINT", Integer::byteValue);
        valCreatorMap.put("SMALLINT", Integer::shortValue);
        valCreatorMap.put("INT", (k) -> k);
        valCreatorMap.put("NUMBER", Integer::longValue);
        valCreatorMap.put("DOUBLE", (k) -> k.doubleValue() + k.doubleValue() / 10000);
        valCreatorMap.put("FLOAT", (k) -> k.floatValue() + k.floatValue() / 10000);
        valCreatorMap.put("DECIMAL", (k) -> BigDecimal.valueOf(k.doubleValue() + k.doubleValue() / 10000));
        valCreatorMap.put("CHAR", (k) -> Integer.toString(k));
        valCreatorMap.put("VARCHAR", (k) -> Integer.toString(k));
        valCreatorMap.put("DATE", (k) -> new java.sql.Date(k.longValue()));
        valCreatorMap.put("TIME", (k) -> new java.sql.Time(k.longValue()));
        valCreatorMap.put("TIMESTAMP", (k) -> new java.sql.Timestamp(k.longValue()));
        valCreatorMap.put("UUID", (k) -> UUID.randomUUID());
    }

    /** Statement that merge one row. */
    private ThreadLocal<PreparedStatement> pstmtThreaded;

    /** Value creators configured by -types option. */
    private Function<Integer, Object>[] valCreators;

    /** {@inheritDoc} */
    @Override protected void setupData() throws Exception {
        String[] dataTypes = args.getStringParameter("types", "DATE").split("\\W+");

        try (Statement stmt = conn.get().createStatement()) {
            stmt.execute(createTableSql(dataTypes));
        }

        pstmtThreaded = newStatement(mergeSql(dataTypes));

        valCreators = valueCreators(dataTypes);

        // Hack delay for the  benchmark running framework (tiden).
        U.sleep(5000);
    }

    /** */
    private String createTableSql(String[] dataTypes) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS TEST (ID INT PRIMARY KEY");

        for (String t : dataTypes)
            sb.append(", val").append(t).append(" ").append(t);

        sb.append(")");

        BenchmarkUtils.println("+++ " + sb.toString());

        return sb.toString();
    }

    /** */
    private String mergeSql(String[] dataTypes) {
        StringBuilder sb = new StringBuilder("MERGE INTO TEST (ID");

        for (String t : dataTypes)
            sb.append(", val").append(t);

        sb.append(") VALUES (?");

        for (String t : dataTypes)
            sb.append(", ?");

        sb.append(")");

        BenchmarkUtils.println("+++ " + sb.toString());

        return sb.toString();
    }

    /** */
    private Function<Integer, Object>[] valueCreators(String[] dataTypes) throws Exception {
        Function<Integer, Object>[] sups = new Function[dataTypes.length];

        for (int i = 0; i < dataTypes.length; ++i) {
            sups[i] = valCreatorMap.get(dataTypes[i].toUpperCase());

            if (Objects.isNull(sups[i]))
                throw new Exception("Unknown type: " + dataTypes[i]);
        }

        return sups;
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        PreparedStatement pstmt = pstmtThreaded.get();

        pstmt.setInt(1, key);

        for (int i = 0; i < valCreators.length; ++i)
            pstmt.setObject(i + 2, valCreators[i].apply(key));

        pstmt.execute();

        if (pstmt.getUpdateCount() != 1)
            throw new Exception("Unexpected update count");

        return true;
    }
}
