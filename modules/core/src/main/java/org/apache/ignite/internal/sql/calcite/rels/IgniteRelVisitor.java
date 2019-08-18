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
package org.apache.ignite.internal.sql.calcite.rels;

import org.apache.calcite.rel.core.Join;

/**
 * TODO: Add class description.
 */
public interface IgniteRelVisitor {
    void onUnionExchange(UnionExchangeRel exch);

    void onRehashingExchange(RehashingExchange exch);

    void onFilter(FilterRel filter);

    void onJoin(Join join);

    void onProject(ProjectRel project);

    void onTableScan(TableScanRel scan);
}
