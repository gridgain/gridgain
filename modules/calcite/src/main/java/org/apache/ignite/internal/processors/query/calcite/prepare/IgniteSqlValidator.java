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

package org.apache.ignite.internal.processors.query.calcite.prepare;

/**
 *
 */

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

/** Validator. */
public class IgniteSqlValidator extends SqlValidatorImpl {
    public IgniteSqlValidator(SqlOperatorTable opTab,
        CalciteCatalogReader catalogReader, JavaTypeFactory typeFactory,
        SqlConformance conformance) {
        super(opTab, catalogReader, typeFactory, conformance);
    }

    /** {@inheritDoc} */
    @Override protected RelDataType getLogicalSourceRowType(
        RelDataType sourceRowType, SqlInsert insert) {
        final RelDataType superType =
            super.getLogicalSourceRowType(sourceRowType, insert);
        return ((JavaTypeFactory) typeFactory).toSql(superType);
    }

    /** {@inheritDoc} */
    @Override protected RelDataType getLogicalTargetRowType(
        RelDataType targetRowType, SqlInsert insert) {
        final RelDataType superType =
            super.getLogicalTargetRowType(targetRowType, insert);
        return ((JavaTypeFactory) typeFactory).toSql(superType);
    }
}
