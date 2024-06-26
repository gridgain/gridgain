/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

import java.util.UUID;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.spi.systemview.view.ContinuousQueryView;

/**
 * KILL CONTINUOUS command.
 *
 * @see QueryMXBean#cancelContinuous(String, String)
 * @see ContinuousQueryView#nodeId()
 * @see ContinuousQueryView#routineId()
 */
public class SqlKillContinuousQueryCommand implements SqlCommand {
    /** KILL CONTINUOUS format message. */
    public static final String KILL_CQ_FORMAT = "Format of the query is " +
        "KILL CONTINUOUS '6fa749ee-7cf8-4635-be10-36a1c75267a7' '123e4567-e89b-12d3-a456-426655440000'";

    /** Origin node id. */
    private UUID originNodeId;

    /** Routine id. */
    private UUID routineId;

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.STRING) {
            originNodeId = UUID.fromString(lex.token());

            if (lex.shift() && lex.tokenType() == SqlLexerTokenType.STRING) {
                routineId = UUID.fromString(lex.token());

                return this;
            }
            else
                throw SqlParserUtils.error(lex, "Expected routine id. " + KILL_CQ_FORMAT);
        }

        throw SqlParserUtils.error(lex, "Expected origin node id. " + KILL_CQ_FORMAT);
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }

    /** @return Routine id. */
    public UUID getRoutineId() {
        return routineId;
    }

    /** @return Origin node id. */
    public UUID getOriginNodeId() {
        return originNodeId;
    }
}
