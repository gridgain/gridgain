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

import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.expression.Expression;
import org.gridgain.internal.h2.result.LocalResult;
import org.gridgain.internal.h2.result.LocalResultFactory;
import org.gridgain.internal.h2.result.LocalResultImpl;

/**
 * Ignite implementation of the H2 local result factory.
 */
public class H2LocalResultFactory extends LocalResultFactory {
    /** {@inheritDoc} */
    @Override public LocalResult create(Session ses, Expression[] expressions, int visibleColCnt, boolean system) {
        if (system)
            return new LocalResultImpl(ses, expressions, visibleColCnt);

        return new H2ManagedLocalResult(ses, expressions, visibleColCnt);
    }

    /** {@inheritDoc} */
    @Override public LocalResult create() {
        return new H2ManagedLocalResult();
    }
}
