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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;

/**
 * KILL CLIENT command.
 *
 * @see org.apache.ignite.internal.visor.client.VisorClientConnectionDropTask
 * @see ClientConnectionView#connectionId()
 */
public class SqlKillClientCommand implements SqlCommand {
    /** Connections id. */
    private Long connectionId;

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        if (lex.shift()) {
            if (lex.tokenType() == SqlLexerTokenType.DEFAULT) {
                String connIdStr = lex.token();

                if (!"ALL".equals(connIdStr))
                    connectionId = Long.parseLong(connIdStr);

                return this;
            }
        }

        throw SqlParserUtils.error(lex, "Expected client connection id or ALL.");
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }

    /** @return Connection id to drop. */
    public Long connectionId() {
        return connectionId;
    }
}