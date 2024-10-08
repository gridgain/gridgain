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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class H2ColumnTypeConversionCheckSelfTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testConversions() throws Exception {
        startGrid(0);

        grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE TBL (id INT PRIMARY KEY, timestamp TIMESTAMP, date DATE, time TIME)"), false);

        assertTrue(isConvertible("TIMESTAMP", Timestamp.class));
        assertTrue(isConvertible("TIMESTAMP", java.sql.Date.class));
        assertTrue(isConvertible("TIMESTAMP", java.util.Date.class));
        assertTrue(isConvertible("TIMESTAMP", LocalDateTime.class));
        assertFalse(isConvertible("TIMESTAMP", Integer.class));

        assertTrue(isConvertible("DATE", java.sql.Date.class));
        assertTrue(isConvertible("DATE", LocalDate.class));
        assertFalse(isConvertible("DATE", Integer.class));

        assertTrue(isConvertible("TIME", java.sql.Time.class));
        assertTrue(isConvertible("TIME", LocalTime.class));
        assertFalse(isConvertible("TIME", Integer.class));
    }

    /** */
    private boolean isConvertible(String colName, Class<?> cls) {
        return grid(0).context().query().getIndexing().isConvertibleToColumnType("PUBLIC", "TBL", colName, cls);
    }
}