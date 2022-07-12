/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.util.Collections.nCopies;
import static org.apache.ignite.internal.commandline.util.PrintingUtils.SimpleType.NUMBER;
import static org.apache.ignite.internal.commandline.util.PrintingUtils.SimpleType.STRING;

public class PrintingUtils {

    public static final String COLUMN_SEPARATOR = "    ";

    /**
     * Prints specified data rows as table.
     *
     * @param titles Titles of the table columns.
     * @param types  Types of the table columns.
     * @param data Table data rows.
     * @param log Logger.
     */
    public static void printTable(List<String> titles, List<SimpleType> types, List<List<?>> data, Logger log) {
        List<Integer> colSzs = titles.stream().map(String::length).collect(Collectors.toList());

        List<List<String>> rows = new ArrayList<>(data.size());

        data.forEach(row -> {
            ListIterator<Integer> colSzIter = colSzs.listIterator();

            rows.add(row.stream().map(val -> {
                String res = String.valueOf(val);

                colSzIter.set(Math.max(colSzIter.next(), res.length()));

                return res;
            }).collect(Collectors.toList()));
        });

        printRow(titles, nCopies(titles.size(), STRING), colSzs, log);

        rows.forEach(row -> printRow(row, types, colSzs, log));
    }

    /**
     * Prints row content with respect to type and size of each column.
     *
     * @param row Row which content should be printed.
     * @param types Column types in sequential order for decent row formatting.
     * @param colSzs Column sizes in sequential order for decent row formatting.
     * @param log Logger.
     */
    private static void printRow(
        Collection<String> row,
        Collection<SimpleType> types,
        Collection<Integer> colSzs,
        Logger log
    ) {
        Iterator<SimpleType> typeIter = types.iterator();
        Iterator<Integer> colSzsIter = colSzs.iterator();

        log.info(row.stream().map(colVal -> {
            SimpleType colType = typeIter.next();

            int colSz = colSzsIter.next();

            String format = colType == SimpleType.DATE || colType == NUMBER ?
                "%" + colSz + "s" :
                "%-" + colSz + "s";

            return String.format(format, colVal);
        }).collect(Collectors.joining(COLUMN_SEPARATOR)));
    }

    /**
     * Represents lightweight type descriptors.
     */
    public enum SimpleType {
        /** Date. */
        DATE,

        /** Number. */
        NUMBER,

        /** String. */
        STRING
    }
}
