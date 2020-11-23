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

import org.apache.ignite.IgniteLogger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Rule for repeating test N times.
 */
public class RepeatRule implements TestRule {
    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param log Logger.
     */
    public RepeatRule(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public Statement apply(Statement statement, Description desc) {
        Statement res = statement;

        Repeat repeat = desc.getAnnotation(Repeat.class);

        if (repeat != null) {
            int times = repeat.value();

            res = new RepeatStatement(log, statement, times);
        }

        return res;
    }

    /** */
    private static class RepeatStatement extends Statement {
        /** Logger. */
        private final IgniteLogger log;

        /** Statement. */
        private final Statement statement;

        /** Repeat. */
        private final int repeat;

        /**
         * @param log Logger.
         * @param statement Statement.
         * @param repeat Repeat.
         */
        public RepeatStatement(IgniteLogger log, Statement statement, int repeat) {
            this.log = log;
            this.statement = statement;
            this.repeat = repeat;
        }

        /** {@inheritDoc} */
        @Override public void evaluate() throws Throwable {
            for (int i = 0; i < repeat; i++) {
                if (log.isInfoEnabled())
                    log.info("Running a repeating test, iteration: " + i);

                try {
                    statement.evaluate();
                }
                catch (Throwable t) {
                    log.error("Falling a repeating test, iteration: " + i, t);

                    throw t;
                }

                if (log.isInfoEnabled())
                    log.info("Completing a repeating test, iteration: " + i);
            }
        }
    }
}
