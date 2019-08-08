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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.jdbc2.JdbcUtils;
import org.apache.ignite.internal.sql.calcite.physical.FilterRule;
import org.apache.ignite.internal.sql.calcite.physical.JoinRule;
import org.apache.ignite.internal.sql.calcite.physical.ProjectRule;
import org.apache.ignite.internal.sql.calcite.physical.TableScanRule;

/**
 * TODO: Add class description.
 */
public class CalciteUtils {

    private CalciteUtils() {
    }

    public static final RelOptRule FILTER_RULE = new FilterRule();

    public static final RelOptRule JOIN_RULE = new JoinRule();

    public static final RelOptRule PROJECT_RULE = new ProjectRule();

    public static final RelOptRule TABLE_SCAN_RULE = new TableScanRule();




    // TODO Java type factory?
    public static SqlTypeName classNameToSqlType(String clsName) {
        int jdbcType = JdbcUtils.type(clsName);

        return SqlTypeName.getNameForJdbcType(jdbcType);
    }
}
