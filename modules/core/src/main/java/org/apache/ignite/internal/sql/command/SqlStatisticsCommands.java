package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;

/**
 * Base class for statistics related commands.
 */
public abstract class SqlStatisticsCommands implements SqlCommand {
    /** Schema name. */
    private String schemaName;


    /** Targets to analyze. */
    protected Collection<StatisticsTarget> targets = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Analyze targets.
     */
    public Collection<StatisticsTarget> targets() {
        return targets;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        while (true) {

            SqlQualifiedName tblQName = parseQualifiedIdentifier(lex);

            String cols[] = parseColumnList(lex);

            targets.add(new StatisticsTarget(tblQName.schemaName(), tblQName.name(), cols));

            if (lex.shift()) {
                if (tryEnd(lex))
                    return this;
            }
            else
                return this;
        }
    }

    /**
     * Test if it is the end of command.
     *
     * @param lex Sql lexer.
     * @return {@code true} if end of command found, {@code false} - otherwise.
     */
    private boolean tryEnd(SqlLexer lex) {
        return lex.tokenType() == SqlLexerTokenType.SEMICOLON;
    }


    /**
     * @param lex Lexer.
     */
    private String[] parseColumnList(SqlLexer lex) {
        if (!lex.shift() || lex.tokenType() == SqlLexerTokenType.SEMICOLON)
            return null;


        if (lex.tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT)
            throw errorUnexpectedToken(lex, "(");

        Set<String> res = new HashSet<>();

        while (true) {
            parseColumn(lex, res);

            if (skipCommaOrRightParenthesis(lex))
                break;
        }
        return (res.isEmpty()) ? null : res.toArray(new String[0]);
    }

    /**
     * @param lex Lexer.
     */
    private void parseColumn(SqlLexer lex, Set<String> cols) {
        String name = parseIdentifier(lex);
        if (!cols.add(name))
            throw error(lex, "Column " + name + " already defined.");
    }
}
