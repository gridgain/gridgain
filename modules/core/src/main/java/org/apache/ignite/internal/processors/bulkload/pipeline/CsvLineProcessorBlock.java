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

package org.apache.ignite.internal.processors.bulkload.pipeline;

import org.apache.ignite.IgniteCheckedException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A {@link PipelineBlock}, which splits line according to CSV format rules and unquotes fields. The next block {@link
 * PipelineBlock#accept(Object, boolean)} is called per-line.
 */
public class CsvLineProcessorBlock extends PipelineBlock<String, String[]> {
    /**
     * Field delimiter pattern.
     */
    private final char fldDelim;

    /**
     * Quote character.
     */
    private final char quoteChars;

    private static final int READER_STATE_UNDEF = 0;
    private static final int READER_STATE_QUOTED = 1;
    private static final int READER_STATE_UNQUOTED = 2;
    private static final int READER_STATE_QUOTED_STARTED = 4;
    private static final int READER_STATE_QUOTED_EMPTY = 8;

    /**
     * Creates a CSV line parser.
     *
     * @param fldDelim   The pattern for the field delimiter.
     * @param quoteChars Quoting character.
     */
    public CsvLineProcessorBlock(Pattern fldDelim, String quoteChars) {
        this.fldDelim = fldDelim.toString().charAt(0);
        this.quoteChars = quoteChars.charAt(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void accept(String input, boolean isLastPortion) throws IgniteCheckedException {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder(256);
        final int length = input.length();
        int copy = 0;
        int readerState = READER_STATE_UNDEF;

        int current = 0;
        int prev = -1;
        int copyStart = 0;
        while (true) {
            if (current == length) {
                if (copy > 0)
                    currentField.append(input, copyStart, copyStart + copy);

                fields.add(currentField.toString());
                break;
            }

            final char c = input.charAt(current++);

            if ((readerState & READER_STATE_QUOTED_STARTED) != 0) {
                if (c == quoteChars) {
                    readerState &= ~READER_STATE_QUOTED_STARTED;

                    if (copy > 0) {
                        currentField.append(input, copyStart, copyStart + copy);
                        copy = 0;
                    }
                    else
                        readerState |= READER_STATE_QUOTED_EMPTY;

                    copyStart = current;
                }
                else
                    copy++;
            }
            else {
                if (c == fldDelim) {
                    if (copy > 0) {
                        currentField.append(input, copyStart, copyStart + copy);
                        copy = 0;
                    }

                    fields.add(currentField.toString());
                    currentField = new StringBuilder();
                    copyStart = current;
                    readerState = READER_STATE_UNDEF;
                }
                else if (c == quoteChars && (readerState & READER_STATE_UNQUOTED) == 0) {
                    readerState = READER_STATE_QUOTED | READER_STATE_QUOTED_STARTED;

                    if (prev == quoteChars)
                        copy++;
                    else
                        copyStart = current;
                }
                else {
                    copy++;

                    if (readerState == READER_STATE_UNDEF)
                        readerState = READER_STATE_UNQUOTED;
                }
            }

            prev = c;
        }

        nextBlock.accept(fields.toArray(new String[0]), isLastPortion);
    }
}
