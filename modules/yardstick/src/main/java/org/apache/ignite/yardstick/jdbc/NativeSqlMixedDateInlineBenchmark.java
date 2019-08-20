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

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

import static org.apache.ignite.yardstick.jdbc.JdbcUtils.fillTableWithIdx;

/**
 * Native sql benchmark that performs insert operations.
 */
public class NativeSqlMixedDateInlineBenchmark extends IgniteAbstractBenchmark {
    /** */
    private static final String TBL_NAME = NativeSqlInsertDeleteBenchmark.class.getSimpleName();

    /** Dummy counter, just for possible jvm optimisation disable purpose. */
    private long resCount;

    /**
     * Benchmarked action that inserts and immediately deletes row.
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long insertKey = nextRandom(args.range()) + 1 + args.range();

        String insertQry = String.format("INSERT INTO %s VALUES (?, ?, ?, ?)", TBL_NAME);

        SqlFieldsQuery insert1 = new SqlFieldsQuery(insertQry);
        insert1.setArgs(insertKey, insertKey + 1, LocalDate.ofEpochDay(insertKey), insertKey + 2);

        ++insertKey;

        SqlFieldsQuery insert2 = new SqlFieldsQuery(insertQry);
        insert2.setArgs(insertKey, insertKey + 1, LocalDate.ofEpochDay(insertKey), insertKey + 2);

        GridQueryProcessor qryProc = ((IgniteEx)ignite()).context().query();

        long selectKey = nextRandom(args.range());

        SqlFieldsQuery select1 = new SqlFieldsQuery(String.format("select * FROM %s where DATE_COL >= ? " +
            "and DATE_COL < ? and DEC_COL= ?", TBL_NAME));
        select1.setArgs(LocalDate.ofEpochDay(selectKey), LocalDate.ofEpochDay(selectKey + 1), selectKey + 1);

        SqlFieldsQuery select2 = new SqlFieldsQuery(String.format("select * FROM %s where DATE_COL = ? " +
            "and DEC_COL= ?", TBL_NAME));
        select1.setArgs(LocalDate.ofEpochDay(selectKey), selectKey + 1);

        SqlFieldsQuery delete1 = new SqlFieldsQuery(String.format("DELETE FROM %s WHERE id = ?", TBL_NAME));
        delete1.setArgs(--insertKey);

        SqlFieldsQuery delete2 = new SqlFieldsQuery(String.format("DELETE FROM %s WHERE DATE_COL = ?", TBL_NAME));
        delete2.setArgs(LocalDate.ofEpochDay(++insertKey));

        try (FieldsQueryCursor<List<?>> insCur1 = qryProc.querySqlFields(insert1, false);
             FieldsQueryCursor<List<?>> insCur2 = qryProc.querySqlFields(insert2, false);
             FieldsQueryCursor<List<?>> selCur1 = qryProc.querySqlFields(select1, false);
             FieldsQueryCursor<List<?>> selCur2 = qryProc.querySqlFields(select2, false);
             FieldsQueryCursor<List<?>> delCur1 = qryProc.querySqlFields(delete1, false);
             FieldsQueryCursor<List<?>> delCur2 = qryProc.querySqlFields(delete2, false)) {

            resCount += insCur1.getAll().size();
            resCount += insCur2.getAll().size();
            resCount += selCur1.getAll().size();
            resCount += selCur2.getAll().size();
            resCount += delCur1.getAll().size();
            resCount += delCur2.getAll().size();
        }
        catch (Exception e) {
            BenchmarkUtils.error("error: ", e);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        fillTableWithIdx(cfg, (IgniteEx)ignite(), this.getClass().getSimpleName(), args.range(), args.atomicMode());
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        super.tearDown();

        BenchmarkUtils.println("Summary results: " + resCount);
    }
}
