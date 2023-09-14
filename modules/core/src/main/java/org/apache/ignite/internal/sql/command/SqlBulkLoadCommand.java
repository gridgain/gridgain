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

import org.apache.ignite.internal.processors.bulkload.BulkLoadCsvFormat;
import org.apache.ignite.internal.processors.bulkload.BulkLoadFormat;
import org.apache.ignite.internal.processors.bulkload.BulkLoadAckClientParameters;
import org.apache.ignite.internal.processors.bulkload.BulkLoadIcebergFormat;
import org.apache.ignite.internal.processors.bulkload.BulkLoadLocation;
import org.apache.ignite.internal.processors.bulkload.BulkLoadLocationFile;
import org.apache.ignite.internal.processors.bulkload.BulkLoadLocationQuery;
import org.apache.ignite.internal.processors.bulkload.BulkLoadLocationTable;
import org.apache.ignite.internal.processors.bulkload.BulkLoadParquetFormat;
import org.apache.ignite.internal.sql.SqlKeyword;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseBoolean;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseInt;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseString;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatches;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * A parser for a COPY command (called 'bulk load' in the code, since word 'copy' is too generic).
 */
public class SqlBulkLoadCommand implements SqlCommand {

    private BulkLoadLocation from;

    private BulkLoadLocation into;

    /** File format. */
    private BulkLoadFormat format;

    /** Packet size (size of portion of a file sent in each sub-request). */
    private Integer packetSize;

    /**
     * Case sensitive properties.
     */
    private Map<String, String> properties;

    /**
     * Parses the command.
     *
     * @param lex The lexer.
     * @return The parsed command object.
     */
    @Override public SqlCommand parse(SqlLexer lex) {
        // COPY keyword is already parsed
        parseLocations(lex);

        if (from == null || into == null) {
            throw error(lex, "(expected both locations: \"FROM\", \"INTO\")");
        }

        parseFormat(lex);

        parseParameters(lex);

        properties = parseProperties(lex);

        return this;
    }

    private void parseLocations(SqlLexer lex) {
        for (int i = 0; i < 2; i++) {
            lex.shift();
            if (matchesKeyword(lex, SqlKeyword.FROM)) {
                from = parseLocation(lex);
            } else if (matchesKeyword(lex, SqlKeyword.INTO)) {
                into = parseLocation(lex);
            } else {
                throw errorUnexpectedToken(lex, SqlKeyword.FROM, SqlKeyword.INTO);
            }
        }
    }

    private static BulkLoadLocation parseLocation(SqlLexer lex) {
        SqlLexerToken lookAhead = lex.lookAhead();
        if (SqlKeyword.isKeyword(lookAhead.token())) {
            throw errorUnexpectedToken(lookAhead, "[file name: string]");
        }

        if (lookAhead.tokenType() == SqlLexerTokenType.STRING) {
            // '/dir/any.file'
            String locFileName = parseFileName(lex);
            return new BulkLoadLocationFile().path(locFileName);

        } else if (lookAhead.tokenType() == SqlLexerTokenType.PARENTHESIS_LEFT) {
            // (query)
            String sql = parseQuery(lex);
            return new BulkLoadLocationQuery().sql(sql);

        } else if (!SqlKeyword.isKeyword(lookAhead.token())) {
            // [<schema>.]<table_name> ( <col_name> [ , <col_name> ... ] )
            SqlQualifiedName tblQName = parseTableName(lex);
            List<String> cols = parseColumns(lex);
            return new BulkLoadLocationTable().tableQualifiedName(tblQName).columns(cols);
        }

        throw errorUnexpectedToken(lookAhead, "[location: 'file', table_name (col_name), (query)]");
    }

    /**
     * Parses the file name.
     *
     * @param lex The lexer.
     */
    private static String parseFileName(SqlLexer lex) {
        if (lex.lookAhead().tokenType() != SqlLexerTokenType.STRING)
            throw errorUnexpectedToken(lex.lookAhead(), "[file name: string]");

        lex.shift();

        return lex.token();
    }

    /**
     * Parses the schema and table names.
     *
     * @param lex The lexer.
     */
    private static SqlQualifiedName parseTableName(SqlLexer lex) {
        return parseQualifiedIdentifier(lex);
    }

    /**
     * Parses the list of columns.
     *
     * @param lex The lexer.
     */
    private static List<String> parseColumns(SqlLexer lex) {
        skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

        List<String> cols = new ArrayList<>();

        do {
            cols.add(parseColumn(lex));
        }
        while (!skipCommaOrRightParenthesis(lex));

        return cols;
    }

    /**
     * Parses column clause.
     *
     * @param lex The lexer.
     * @return The column name.
     */
    private static String parseColumn(SqlLexer lex) {
        return parseIdentifier(lex);
    }

    private static String parseQuery(SqlLexer lex) {
        if (lex.lookAhead().tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT)
            throw errorUnexpectedToken(lex.lookAhead(), "[query: parenthesis]");

        lex.shift();
        ArrayDeque<SqlLexerTokenType> stack = new ArrayDeque<>();
        stack.push(lex.tokenType());

        int startInclusive = lex.position();
        do {
            lex.shift();
            SqlLexerTokenType tokenType = lex.tokenType();
            if (tokenType == SqlLexerTokenType.PARENTHESIS_LEFT) {
                stack.push(tokenType);
            } else if (tokenType == SqlLexerTokenType.PARENTHESIS_RIGHT) {
                stack.pop();
            }
        } while (!(stack.isEmpty() || lex.eod()));
        int endExclusive = lex.tokenPosition();

        if (!stack.isEmpty()) {
            throw errorUnexpectedToken(lex.lookAhead(), "[query: parenthesis]");
        }

        return lex.sql().substring(startInclusive, endExclusive);
    }

    /**
     * Parses the format clause.
     *
     * @param lex The lexer.
     */
    private void parseFormat(SqlLexer lex) {
        skipIfMatchesKeyword(lex, SqlKeyword.FORMAT);

        String name = parseIdentifier(lex);

        switch (name.toUpperCase()) {
            case "CSV":
                BulkLoadCsvFormat fmt = new BulkLoadCsvFormat();

                // IGNITE-7537 will introduce user-defined values
                fmt.lineSeparator(BulkLoadCsvFormat.DEFAULT_LINE_SEPARATOR);
                fmt.fieldSeparator(BulkLoadCsvFormat.DEFAULT_FIELD_SEPARATOR);
                fmt.quoteChars(BulkLoadCsvFormat.DEFAULT_QUOTE_CHARS);
                fmt.commentChars(BulkLoadCsvFormat.DEFAULT_COMMENT_CHARS);
                fmt.escapeChars(BulkLoadCsvFormat.DEFAULT_ESCAPE_CHARS);
                fmt.nullString(BulkLoadCsvFormat.DEFAULT_NULL_STRING);
                fmt.trim(BulkLoadCsvFormat.DEFAULT_TRIM_SPACES);
                fmt.inputCharsetName(BulkLoadCsvFormat.DFLT_INPUT_CHARSET.toString());

                parseCsvOptions(lex, fmt);

                validateCsvParserFormat(lex, fmt);

                format = fmt;

                break;
            case "PARQUET":
                BulkLoadParquetFormat parquetFormat = new BulkLoadParquetFormat();

                parseParquetOptions(lex, parquetFormat);

                format = parquetFormat;

                break;
            case "ICEBERG":
                format = new BulkLoadIcebergFormat();

                break;
            default:
                throw error(lex, "Unknown format name: " + name);
        }
    }

    /**
     * Parses CSV format options.
     *
     * @param lex The lexer.
     * @param format CSV format object to configure.
     */
    private void parseCsvOptions(SqlLexer lex, BulkLoadCsvFormat format) {
        while (lex.lookAhead().tokenType() == SqlLexerTokenType.DEFAULT) {
            switch (lex.lookAhead().token()) {
                case SqlKeyword.CHARSET: {
                    lex.shift();

                    String charsetName = parseString(lex);

                    format.inputCharsetName(charsetName);

                    break;
                }

                case SqlKeyword.DELIMITER: {
                    lex.shift();

                    String delimiter = parseString(lex);

                    format.fieldSeparator(delimiter);

                    break;
                }

                case SqlKeyword.TRIM: {
                    lex.shift();

                    Boolean trim = parseBoolean(lex);

                    format.trim(trim);

                    break;
                }

                case SqlKeyword.NULLSTRING: {
                    lex.shift();

                    String nullString = parseString(lex);

                    format.nullString(nullString);

                    break;
                }

                default:
                    return;
            }
        }
    }

    /**
     * Parses Parquet format options.
     *
     * @param lex The lexer.
     * @param format Parquet format object to configure.
     */
    private void parseParquetOptions(SqlLexer lex, BulkLoadParquetFormat format) {
        while (lex.lookAhead().tokenType() == SqlLexerTokenType.DEFAULT) {
            switch (lex.lookAhead().token()) {
                case SqlKeyword.PATTERN: {
                    lex.shift();

                    String pattern = parseString(lex);
                    format.pattern(pattern);

                    break;
                }

                default:
                    return;
            }
        }
    }

    /**
     * Parses the optional parameters.
     *
     * @param lex The lexer.
     */
    private void parseParameters(SqlLexer lex) {
        while (lex.lookAhead().tokenType() == SqlLexerTokenType.DEFAULT) {
            switch (lex.lookAhead().token()) {
                case SqlKeyword.PACKET_SIZE:
                    lex.shift();

                    int size = parseInt(lex);

                    if (!BulkLoadAckClientParameters.isValidPacketSize(size))
                        throw error(lex, BulkLoadAckClientParameters.packetSizeErrorMesssage(size));

                    packetSize = size;

                    break;

                default:
                    return;
            }
        }
    }

    /**
     * Parses the optional case-sensitive properties with syntax PROPERTIES ('k1' = 'v1', 'k2' = 'v2', ...).
     * @param lex The lexer.
     * @return Immutable properties.
     */
    private static Map<String, String> parseProperties(SqlLexer lex) {
        if (!"PROPERTIES".equals(lex.lookAhead().token())) {
            return Collections.emptyMap();
        }

        skipIfMatchesKeyword(lex, "PROPERTIES");
        skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

        Map<String, String> props = new HashMap<>();

        do {
            if (lex.lookAhead().tokenType() == SqlLexerTokenType.PARENTHESIS_RIGHT) {
                continue;
            }
            String key = lex.lookAhead().token();
            lex.shift();
            skipIfMatchesKeyword(lex, "=");
            String value = lex.lookAhead().token();
            lex.shift();
            props.put(key, value);
        }
        while (!skipCommaOrRightParenthesis(lex));
        lex.shift();

        return Collections.unmodifiableMap(props);
    }

    /**
     * Parses the optional parameters.
     *
     * @param lex The lexer.
     * @param format CSV format object to validate.
     */
    private void validateCsvParserFormat(SqlLexer lex, BulkLoadCsvFormat format) {
        String delimiter = format.fieldSeparator().toString();
        String quoteChars = format.quoteChars();

        if (delimiter.length() > 1 || quoteChars.length() > 1)
            throw error(lex, "Delimiter or quote chars must consist of single character: delim is '" + delimiter
                + "', quote char is '" + quoteChars + "'");

        if (delimiter.equals(quoteChars))
            throw error(lex, "Invalid delimiter or quote chars: delim is '" + delimiter
                + "', quote char is '" + quoteChars + "'");
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        if (from instanceof BulkLoadLocationTable && null == ((BulkLoadLocationTable) from).schemaName()) {
            ((BulkLoadLocationTable) from).qualifiedName().schemaName(schemaName, true);
        }
        if (into instanceof BulkLoadLocationTable && null == ((BulkLoadLocationTable) into).schemaName()) {
            ((BulkLoadLocationTable) into).qualifiedName().schemaName(schemaName, true);
        }
    }

    /**
     * Returns the file format.
     *
     * @return The file format.
     */
    public BulkLoadFormat format() {
        return format;
    }

    /**
     * Returns the packet size.
     *
     * @return The packet size.
     */
    public Integer packetSize() {
        return packetSize;
    }

    /**
     * Sets the packet size.
     *
     * @param packetSize The packet size.
     */
    public void packetSize(int packetSize) {
        this.packetSize = packetSize;
    }

    /**
     * Location to COPY FROM.
     * @return location
     */
    public BulkLoadLocation from() {
        return from;
    }

    /**
     * Location to COPY INTO.
     * @return location
     */
    public BulkLoadLocation into() {
        return into;
    }

    /**
     * Propertes (case-sensitive) passed to command.
     * Syntax: PROPERTIES ('k1' = 'v1', 'k2' = 'v2', ...)
     * Properties keys/values are case-sensitive.
     * @return Immutable properties.
     */
    public Map<String, String> properties() {
        return properties;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlBulkLoadCommand.class, this);
    }
}
