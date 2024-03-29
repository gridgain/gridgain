/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.managers.systemview.walker;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.processors.query.stat.view.StatisticsColumnLocalDataView;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link StatisticsColumnLocalDataView} attributes walker.
 * 
 * @see StatisticsColumnLocalDataView
 */
public class StatisticsColumnLocalDataViewWalker implements SystemViewRowAttributeWalker<StatisticsColumnLocalDataView> {
    /** Filter key for attribute "schema" */
    public static final String SCHEMA_FILTER = "schema";

    /** Filter key for attribute "type" */
    public static final String TYPE_FILTER = "type";

    /** Filter key for attribute "name" */
    public static final String NAME_FILTER = "name";

    /** Filter key for attribute "column" */
    public static final String COLUMN_FILTER = "column";

    /** List of filtrable attributes. */
    private static final List<String> FILTRABLE_ATTRS = Collections.unmodifiableList(F.asList(
        "schema", "type", "name", "column"
    ));

    /** {@inheritDoc} */
    @Override public List<String> filtrableAttributes() {
        return FILTRABLE_ATTRS;
    }

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "schema", String.class);
        v.accept(1, "type", String.class);
        v.accept(2, "name", String.class);
        v.accept(3, "column", String.class);
        v.accept(4, "rowsCount", long.class);
        v.accept(5, "distinct", long.class);
        v.accept(6, "nulls", long.class);
        v.accept(7, "total", long.class);
        v.accept(8, "size", int.class);
        v.accept(9, "version", long.class);
        v.accept(10, "lastUpdateTime", Timestamp.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(StatisticsColumnLocalDataView row, AttributeWithValueVisitor v) {
        v.accept(0, "schema", String.class, row.schema());
        v.accept(1, "type", String.class, row.type());
        v.accept(2, "name", String.class, row.name());
        v.accept(3, "column", String.class, row.column());
        v.acceptLong(4, "rowsCount", row.rowsCount());
        v.acceptLong(5, "distinct", row.distinct());
        v.acceptLong(6, "nulls", row.nulls());
        v.acceptLong(7, "total", row.total());
        v.acceptInt(8, "size", row.size());
        v.acceptLong(9, "version", row.version());
        v.accept(10, "lastUpdateTime", Timestamp.class, row.lastUpdateTime());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 11;
    }
}
