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

import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.sql.SqlKeyword;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.sql.SqlLexerTokenType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * ANALYZE command to mark object for statistics collection.
 */
public class SqlAnalyzeCommand extends SqlStatisticsCommands {
    /** OBSOLESCENCE_MAP_PERCENT parameter name. */
    public static final String MAX_CHANGED_PARTITION_ROWS_PERCENT = "MAX_CHANGED_PARTITION_ROWS_PERCENT";

    /** Targets to analyze. */
    protected List<StatisticsObjectConfiguration> configs = new ArrayList<>();
    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        while (true) {

            SqlQualifiedName tblQName = parseQualifiedIdentifier(lex);

            String[] cols = parseColumnList(lex, true);
            Map<String, String> params = parseParams(lex);

            StatisticsTarget target = new StatisticsTarget(tblQName.schemaName(), tblQName.name(), cols);
            configs.add(buildConfig(target, params));

            if (tryEnd(lex))
                return this;
        }
    }

    /**
     * Build statistics object configuration from command arguments.
     *
     * @param target Statistics target.
     * @param params Map of parameter to value strings.
     * @return Statistics object configuration.
     * @throws IgniteSQLException In case of unexpected parameter.
     */
    public StatisticsObjectConfiguration buildConfig(StatisticsTarget target, Map<String, String> params)
        throws IgniteSQLException {
        byte maxChangedRows = getByteOrDefault(params, MAX_CHANGED_PARTITION_ROWS_PERCENT,
            StatisticsObjectConfiguration.DEFAULT_OBSOLESCENCE_MAX_PERCENT);

        if (!params.isEmpty())
            throw new IgniteSQLException("");

        List<StatisticsColumnConfiguration> colCfgs = Arrays.stream(target.columns())
            .map(StatisticsColumnConfiguration::new).collect(Collectors.toList());

        return new StatisticsObjectConfiguration(target.key(), colCfgs, maxChangedRows);
    }

    private static byte getByteOrDefault(Map<String,String> map, String key, byte defaultValue) {
        String value = map.remove(key);
        return (value==null) ? defaultValue : Byte.valueOf(value);
    }

    /**
     * @return Target to params map.
     */
    public Collection<StatisticsObjectConfiguration> configurations() {
        return configs;
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
