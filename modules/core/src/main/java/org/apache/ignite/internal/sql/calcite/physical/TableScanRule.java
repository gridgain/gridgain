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
package org.apache.ignite.internal.sql.calcite.physical;

import java.util.function.Predicate;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.Table;
import org.apache.ignite.internal.sql.calcite.IgniteConvention;

/**
 * TODO: Add class description.
 */
public class TableScanRule extends ConverterRule {

    public TableScanRule() {
        super(LogicalTableScan.class, (Predicate<RelNode>) r -> true,
            Convention.NONE, IgniteConvention.INSTANCE, RelFactories.LOGICAL_BUILDER,
            "IgniteTableScanRule");
    }

    @Override public RelNode convert(RelNode rel) {
        LogicalTableScan scan = (LogicalTableScan) rel;
        final RelOptTable relOptTable = scan.getTable();
        final Table table = relOptTable.unwrap(Table.class); // TODO use this to change the distribution trait
        System.out.println("TableScanRule table=" + table);

        return new TableScanRel(scan.getCluster(),
            scan.getTraitSet().replace(IgniteConvention.INSTANCE),
            relOptTable);
    }
}
