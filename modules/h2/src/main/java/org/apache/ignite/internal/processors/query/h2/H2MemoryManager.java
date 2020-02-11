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
package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import org.h2.command.dml.GroupByData;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.ResultExternal;
import org.h2.result.SortOrder;

/**
 * Memory manager for H2 query memory tracking and disk offloading.
 */
public interface H2MemoryManager {

    /**
     * Group-by data fabric method.
     *
     * @param ses Session.
     * @param expressions Expressions.
     * @param isGrpQry Group query flag.
     * @param grpIdx Group-by fields indexes.
     * @return Group-by data.
     */
    GroupByData newGroupByDataInstance(Session ses, ArrayList<Expression> expressions, boolean isGrpQry, int[] grpIdx);

    /**
     * Creates plain external result.
     *
     * @param session Session.
     * @return New external result.
     */
    ResultExternal createPlainExternalResult(Session session);

    /**
     * Creates sorted external result.
     *
     * @param session Session.
     * @param distinct Distinct flag.
     * @param indexes Distinct indexes.
     * @param visibleColCnt Visible column count.
     * @param sort Sort.
     * @param rowCount Row count.
     * @return Sorted external result.
     */
    ResultExternal createSortedExternalResult(Session session, boolean distinct, int[] indexes, int visibleColCnt,
        SortOrder sort, int rowCount);
}
