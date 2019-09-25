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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.function.Predicate;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.QueryContext;

/**
 *
 */
public final class Commons {
    private Commons(){}

    public static IgniteLogger log(RelNode rel) {
        return log(rel.getCluster().getPlanner().getContext());
    }

    public static IgniteLogger log(Context ctx) {
        return ctx.unwrap(IgniteLogger.class);
    }

    public static Context convert(QueryContext ctx) {
        return ctx == null ? Contexts.empty() : Contexts.of(ctx.unwrap(Object[].class));
    }

    public static <T> Predicate<T> any() {
        return obj -> true;
    }
}
