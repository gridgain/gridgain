/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.sql.SqlKeyword;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.sql.SqlLexerTokenType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * ANALYZE command to mark object for statistics collection.
 */
public class SqlAnalyzeCommand extends SqlStatisticsCommands {
    /** Targets to analyze. */
    protected Map<StatisticsTarget, Map<String, String>> targets = new HashMap<>();

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        while (true) {

            SqlQualifiedName tblQName = parseQualifiedIdentifier(lex);

            String[] cols = parseColumnList(lex, true);
            Map<String, String> params = parseParams(lex);

            targets.put(new StatisticsTarget(tblQName.schemaName(), tblQName.name(), cols), params);

            if (tryEnd(lex))
                return this;
        }
    }

    /**
     * @return Target to params map.
     */
    public Map<StatisticsTarget, Map<String, String>> targetsMap() {
        return targets;
    }

    /**
     * Pars param including WITH keyword.
     *
     * @param lex Lexer to use.
     * @return Map of parameters.
     */
    private Map<String, String> parseParams(SqlLexer lex) {
        SqlLexerToken nextTok = lex.lookAhead();
        if (nextTok.token() == null || nextTok.tokenType() == SqlLexerTokenType.SEMICOLON
            || nextTok.tokenType() == SqlLexerTokenType.COMMA)
            return null;

        skipIfMatchesKeyword(lex, SqlKeyword.WITH);

        lex.shift();

        String paramsStr = lex.token();

        String[] params =  paramsStr.split(",");

        Map<String, String> res = new HashMap<>(params.length);
        for (String param : params) {
            int p = param.indexOf("=");
            res.put(param.substring(0, p), param.substring(p + 1));
        }

        return res;
    }
}
