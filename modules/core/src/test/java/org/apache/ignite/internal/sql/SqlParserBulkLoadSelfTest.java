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

package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.processors.bulkload.BulkLoadLocationQuery;
import org.apache.ignite.internal.processors.bulkload.BulkLoadLocationTable;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.junit.Test;

/**
 * Tests for SQL parser: COPY command.
 */
public class SqlParserBulkLoadSelfTest extends SqlParserAbstractSelfTest {
    /** Tests for COPY command. */
    @Test
    public void testCopy() {
        assertParseError(null,
            "copy grom 'any.file' into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"GROM\" (expected: \"FROM\")");

        assertParseError(null,
            "copy from into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"INTO\" (expected: \"[file name: string]\"");

        assertParseError(null,
            "copy from unquoted into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"INTO\" (expected: \"(\")");

        assertParseError(null,
            "copy from unquoted.file into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"INTO\" (expected: \"(\")");

        new SqlParser(null,
            "copy from '' into Person (_key, age, firstName, lastName) format csv")
            .nextCommand();

        // File

        new SqlParser(null,
            "copy from 'd:/copy/from/into/format.csv' into Person (_key, age, firstName, lastName) format csv")
            .nextCommand();

        new SqlParser(null,
            "copy from '/into' into Person (_key, age, firstName, lastName) format csv")
            .nextCommand();

        new SqlParser(null,
            "copy from 'into' into Person (_key, age, firstName, lastName) format csv")
            .nextCommand();

        new SqlParser(null,
            "copy from 'into' into Person (_key, age, firstName, lastName) format csv delimiter ','")
            .nextCommand();

        new SqlParser(null,
            "copy from 'into' into Person (_key, age, firstName, lastName) format csv trim on")
            .nextCommand();

        new SqlParser(null,
            "copy from 'into' into Person (_key, age, firstName, lastName) format csv nullstring 'a'")
            .nextCommand();

        assertParseError(null,
            "copy from 'into' into Person (_key, age, firstName, lastName) format csv delimiter '\"'",
            "Invalid delimiter or quote chars: delim is '\"', quote char is '\"'");

        assertParseError(null,
            "copy from 'into' into Person (_key, age, firstName, lastName) format csv delimiter ',.'",
            "Delimiter or quote chars must consist of single character: delim is ',.', quote char is '\"'");

        assertParseError(null,
            "copy from 'any.file' to Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"TO\" (expected: \"INTO\")");

        // Column list

        assertParseError(null,
            "copy from '" +
                "any.file' into Person () format csv",
            "Unexpected token: \")\" (expected: \"[identifier]\")");

        assertParseError(null,
            "copy from 'any.file' into Person (,) format csv",
            "Unexpected token: \",\" (expected: \"[identifier]\")");

        assertParseError(null,
            "copy from 'any.file' into Person format csv",
            "Unexpected token: \"FORMAT\" (expected: \"(\")");

        // FORMAT

        assertParseError(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName)",
            "Unexpected end of command (expected: \"FORMAT\")");

        assertParseError(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format lsd",
            "Unknown format name: LSD");

        // FORMAT CSV CHARSET

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv")
            .nextCommand();

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset" +
            " 'utf-8'")
            .nextCommand();

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset" +
            " 'UTF-8'")
            .nextCommand();

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset" +
            " 'UtF-8'")
            .nextCommand();

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset" +
            " 'windows-1251'")
            .nextCommand();

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset" +
            " 'ISO-2022-JP'")
            .nextCommand();

        assertParseError(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset ",
            "Unexpected end of command (expected: \"[string]\")");
    }

    @Test
    public void testCopySubquery() {
        String subquery = "select * from (select * from Person) as p";

        SqlCommand cmd = new SqlParser(null,
                "copy from (" + subquery + ") into 'any.file' format csv")
                .nextCommand();
        String actual = ((BulkLoadLocationQuery) ((SqlBulkLoadCommand) cmd).from()).sql();
        assertEquals(subquery, actual);

        assertParseError(null,
                "copy from (select * from (select * from Person as p into 'any.file' format csv",
                "Unexpected end of command (expected: \"[query: parenthesis]\")");

        assertParseError(null,
                "copy from select _key from (select * from Person) as p) into 'any.file' format csv",
                "Unexpected token: \"_KEY\" (expected: \"(\"");
    }

    @Test
    public void testCopyTable() {
        SqlCommand cmd = new SqlParser(null, "COPY FROM \"schema\".\"person\" (_key, id) into 'any.file' format csv")
                .nextCommand();
        BulkLoadLocationTable from = (BulkLoadLocationTable) ((SqlBulkLoadCommand) cmd).from();
        assertTrue(from.isSchemaNameQuoted());
        assertTrue(from.isTableNameQuoted());

        cmd = new SqlParser(null, "COPY FROM schema.person (_key, id) into 'any.file' format csv")
                .nextCommand();
        from = (BulkLoadLocationTable) ((SqlBulkLoadCommand) cmd).from();
        assertFalse(from.isSchemaNameQuoted());
        assertFalse(from.isTableNameQuoted());
    }
}
