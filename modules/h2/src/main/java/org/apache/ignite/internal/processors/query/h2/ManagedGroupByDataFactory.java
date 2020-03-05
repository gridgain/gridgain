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

/**
 * Factory for H2 group by data.
 */
public interface ManagedGroupByDataFactory {

    /**
     * Group-by data fabric method.
     *
     * @param ses Session.
     * @param expressions Expressions.
     * @param isGrpQry Group query flag.
     * @param grpIdx Group-by fields indexes.
     * @return Group-by data.
     */
    GroupByData newManagedGroupByData(Session ses, ArrayList<Expression> expressions, boolean isGrpQry, int[] grpIdx);
}
