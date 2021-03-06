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

package org.apache.ignite.testframework.junits;

import org.jetbrains.annotations.NotNull;
import org.junit.runners.model.Statement;

/**
 * Utility class to convert lambdas into {@link Statement} objects.
 *
 * @see Statement
 */
public class DelegatingJUnitStatement extends Statement {
    /** Object to delegate statements body to. */
    private final StatementEx delegate;

    /**
     * @param delegate Object to delegate statements body to.
     * @return {@link StatementEx} converted to {@link Statement}.
     */
    public static Statement wrap(@NotNull StatementEx delegate) {
        return new DelegatingJUnitStatement(delegate);
    }

    /**
     * @param delegate Object to delegate statements body to.
     */
    private DelegatingJUnitStatement(@NotNull StatementEx delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public void evaluate() throws Throwable {
        delegate.evaluate();
    }

    /**
     * Functional version of {@link Statement} abstract class.
     *
     * @see Statement
     */
    @FunctionalInterface
    public static interface StatementEx {
        /**
         * @see Statement#evaluate()
         */
        void evaluate() throws Throwable;
    }
}
