/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.sql.calcite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.sql.calcite.CalciteUtils.classNameToSqlType;

/**
 * TODO: Add class description.
 */
public class IgniteTable extends AbstractTable implements ScannableTable/*, ProjectableFilterableTable*/ { // TODO uncomment

    private long rowCnt;

    /** Unique columns. */
    List<ImmutableBitSet> keys;

    List<Column> cols;

    public IgniteTable(QueryEntity entity) {
        keys = Collections.singletonList(ImmutableBitSet.builder().set(0).build()); // PK is at the 0 position.
        cols = new ArrayList<>(entity.getFields().size() + 1); // Fields + _key;
        cols.add(new Column(0, entity.getKeyFieldName(), classNameToSqlType(entity.getKeyType())));

        String keyName = entity.getKeyFieldName();

        int colCnt = 0;

        if (!F.isEmpty(entity.getFields())) {
            for (Map.Entry<String, String> fld : entity.getFields().entrySet()) {
                if (keyName.equals(fld.getKey()))
                    continue; // Skip key field.
                cols.add(new Column(++colCnt, fld.getKey(), classNameToSqlType(fld.getValue())));
            }
        }
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        for (Column col : cols) {
            builder.add(col.name, col.type);
        }

        return builder.build();
    }

    @Override public Statistic getStatistic() {
        return Statistics.of(rowCnt, keys);
    }

    public void incrementRowCount() {
        rowCnt++;
    }

    public void decrementRowCount() {
        rowCnt--;
    }

//    @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
//        throw new UnsupportedOperationException();
//    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
        throw new UnsupportedOperationException();
    }
}
