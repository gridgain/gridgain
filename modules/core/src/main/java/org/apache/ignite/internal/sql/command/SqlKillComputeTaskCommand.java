/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.spi.systemview.view.ComputeJobView;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;

/**
 * KILL COMPUTE_TASK command.
 *
 * @see ComputeMXBean#cancel(String)
 * @see ComputeJobView#sessionId()
 * @see ComputeTaskView#sessionId()
 */
public class SqlKillComputeTaskCommand implements SqlCommand {
    /** Session id. */
    private IgniteUuid sesId;

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        if (lex.shift()) {
            if (lex.tokenType() == SqlLexerTokenType.STRING) {
                sesId = IgniteUuid.fromString(lex.token());

                return this;
            }
        }

        throw SqlParserUtils.error(lex, "Expected session id.");
    }

    /** @return Session id. */
    public IgniteUuid getSessionId() {
        return sesId;
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }
}
