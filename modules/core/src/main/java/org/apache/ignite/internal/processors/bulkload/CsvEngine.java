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

package org.apache.ignite.internal.processors.bulkload;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Logic for importing data from CSV
 */
public class CsvEngine implements AutoCloseable {

    /** CSV reader */
    private final CSVReader csvReader;

    /** iterator for iterating over CSV file */
    private Iterator<String[]> iterator;

    /** format options */
    private final BulkLoadCsvFormat fmt;

    /** Size of a batch to read from CSV file */
    private static final int BATCH_SIZE = 100;

    public CsvEngine(String path, BulkLoadCsvFormat fmt) throws SQLException, IOException {
        this.fmt = fmt;

        CSVParser csvParser = new CSVParserBuilder().withSeparator(fmt.fieldSeparator().charAt(0))
                .withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
                .build();

        Charset charset = Charset.forName(fmt.inputCharsetName());

        csvReader = new CSVReaderBuilder(Files.newBufferedReader(Paths.get(path), charset ))
                .withCSVParser(csvParser).build();

        try {
            iterator = csvReader.iterator();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    /**
     *
     * @return
     * @throws SQLException
     */
    public List<List<String>> getBatch() throws SQLException {
        try {
            List<List<String>> res = new ArrayList<>();
            for (int i = 0; i < BATCH_SIZE; i++) {
                if (iterator.hasNext())
                    res.add(Arrays.asList(processRow(iterator.next())));
                else
                    return res;
            }
            return res;
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    private String[] processRow(String[] row) {
        if (fmt.nullString() != null) {
            for (int i = 0; i < row.length; i++) {
                if (row[i] != null) {
                    row[i] = processTrim(row[i]);
                    row[i] = processNullString(row[i]);
                }
            }
        }
        return row;
    }

    private String processNullString(String val) {
        if (fmt.nullString().equals(val))
            return null;
        return val;
    }

    private String processTrim(String val) {
        if (fmt.trim())
            return val.trim();
        else
            return val;
    }

    @Override public void close() throws Exception {
        csvReader.close();
    }
}
