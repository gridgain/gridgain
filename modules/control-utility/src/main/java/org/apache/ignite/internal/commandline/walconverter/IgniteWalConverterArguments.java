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

package org.apache.ignite.internal.commandline.walconverter;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.BINARY_METADATA_DIR;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.HAS_TEXT;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.INCLUDE_SENSITIVE;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.MARSHALLER_MAPPING_DIR;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.PAGE_SIZE;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.PRINT_STAT;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.RECORD_TYPES;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.SKIP_CRC;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.UNWRAP_BINARY;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.WAL_ARCHIVE_DIR;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.WAL_DIR;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.WAL_TIME_FROM_MILLIS;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.Args.WAL_TIME_TO_MILLIS;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;

/**
 * Parameters for IgniteWalConverter with parsed and validated.
 */
public class IgniteWalConverterArguments {
    /** Path to dir with wal files. */
    private final File walDir;

    /** Path to dir with archive wal files. */
    private final File walArchiveDir;

    /** Size of pages, which was selected for file store (1024, 2048, 4096, etc). */
    private final int pageSize;

    /** Path to binary metadata dir. */
    private final File binaryMetadataDir;

    /** Path to marshaller dir. */
    private final File marshallerMappingDir;

    /** Keep binary flag. */
    private final boolean unwrapBinary;

    /** WAL record types (TX_RECORD, DATA_RECORD, etc). */
    private final Set<WALRecord.RecordType> recordTypes;

    /** The start time interval for the record time in milliseconds. */
    private final Long fromTime;

    /** The end time interval for the record time in milliseconds. */
    private final Long toTime;

    /** Filter by substring in the WAL record. */
    private final String hasText;

    /** Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5). */
    private final ProcessSensitiveData includeSensitive;

    /** Write summary statistics for WAL */
    private final boolean printStat;

    /** Skip CRC calculation/check flag */
    private final boolean skipCrc;

    /**
     * @param walDir                        Path to dir with wal files.
     * @param walArchiveDir                 Path to dir with archive wal files.
     * @param pageSize                      Size of pages, which was selected for file store (1024, 2048, 4096, etc).
     * @param binaryMetadataDir             Path to binary metadata dir.
     * @param marshallerMappingDir          Path to marshaller dir.
     * @param unwrapBinary                  Unwrap binary non-primitive objects.
     * @param recordTypes                   WAL record types (TX_RECORD, DATA_RECORD, etc).
     * @param fromTime                      The start time interval for the record time in milliseconds.
     * @param toTime                        The end time interval for the record time in milliseconds.
     * @param hasText                       Filter by substring in the WAL record.
     * @param includeSensitive              Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5).
     * @param printStat                     Write summary statistics for WAL.
     * @param skipCrc                       Skip CRC calculation/check flag.
     */
    public IgniteWalConverterArguments(
        File walDir,
        File walArchiveDir,
        int pageSize,
        File binaryMetadataDir,
        File marshallerMappingDir,
        boolean unwrapBinary,
        Set<WALRecord.RecordType> recordTypes,
        Long fromTime,
        Long toTime,
        String hasText,
        ProcessSensitiveData includeSensitive,
        boolean printStat,
        boolean skipCrc
    ) {
        this.walDir = walDir;
        this.walArchiveDir = walArchiveDir;
        this.pageSize = pageSize;
        this.binaryMetadataDir = binaryMetadataDir;
        this.marshallerMappingDir = marshallerMappingDir;
        this.unwrapBinary = unwrapBinary;
        this.recordTypes = recordTypes;
        this.fromTime = fromTime;
        this.toTime = toTime;
        this.hasText = hasText;
        this.includeSensitive = includeSensitive;
        this.printStat = printStat;
        this.skipCrc = skipCrc;
    }

    /**
     * Path to dir with wal files.
     *
     * @return walDir
     */
    public File getWalDir() {
        return walDir;
    }

    /**
     * Path to dir with archive wal files.
     *
     * @return walArchiveDir
     */
    public File getWalArchiveDir() {
        return walArchiveDir;
    }

    /**
     * Size of pages, which was selected for file store (1024, 2048, 4096, etc).
     *
     * @return pageSize
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Path to binary metadata dir.
     *
     * @return binaryMetadataFileStoreD
     */
    public File getBinaryMetadataDir() {
        return binaryMetadataDir;
    }

    /**
     * Path to marshaller dir.
     *
     * @return marshallerMappingFileStoreD
     */
    public File getMarshallerMappingDir() {
        return marshallerMappingDir;
    }

    /**
     * Keep binary flag.
     *
     * @return keepBina
     */
    public boolean isUnwrapBinary() {
        return unwrapBinary;
    }

    /**
     * WAL record types (TX_RECORD, DATA_RECORD, etc).
     *
     * @return recordTypes
     */
    public Set<WALRecord.RecordType> getRecordTypes() {
        return recordTypes;
    }

    /**
     * The start time interval for the record time in milliseconds.
     *
     * @return fromTime
     */
    public Long getFromTime() {
        return fromTime;
    }

    /**
     * The end time interval for the record time in milliseconds.
     *
     * @return toTime
     */
    public Long getToTime() {
        return toTime;
    }

    /**
     * Filter by substring in the WAL record.
     *
     * @return Filter substring.
     */
    public String hasText() {
        return hasText;
    }

    /**
     * Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5).
     *
     * @return Mode of sensitive data including.
     */
    public ProcessSensitiveData includeSensitive() {
        return includeSensitive;
    }

    /**
     * Write summary statistics for WAL.
     *
     * @return printStat
     */
    public boolean isPrintStat() {
        return printStat;
    }

    /**
     * Skip CRC calculation/check flag.
     *
     * @return skipCrc
     */
    public boolean isSkipCrc() {
        return skipCrc;
    }

    /**
     * @param filePath Path to the file.
     * @return File, or {@code null} if {@code filePath} == {@code null}.
     */
    public static @Nullable File file(
        @Nullable String filePath,
        boolean checkExists,
        boolean checkIsDirectory
    ) {
        if (filePath == null)
            return null;
        else {
            File file = new File(filePath);

            if (checkExists && !file.exists())
                throw new IllegalArgumentException("File/directory '" + filePath + "' does not exist.");

            if (checkIsDirectory && !file.isDirectory())
                throw new IllegalArgumentException("File '" + filePath + "' must be directory.");

            return file;
        }
    }

    /**
     * Check record types.
     *
     * @param recordTypesStrArray String array to check.
     * @return Record types array.
     */
    private static Set<WALRecord.RecordType> checkRecordTypes(String[] recordTypesStrArray) {
        final SortedSet<String> unknownRecordTypes = new TreeSet<>();

        final Set<WALRecord.RecordType> recordTypes = new HashSet<>();

        if (recordTypesStrArray != null) {
            for (String recordTypeStr : recordTypesStrArray) {
                try {
                    recordTypes.add(WALRecord.RecordType.valueOf(recordTypeStr));
                }
                catch (Exception e) {
                    unknownRecordTypes.add(recordTypeStr);
                }
            }

            if (!unknownRecordTypes.isEmpty()) {
                throw new IllegalArgumentException("Unknown record types: " + unknownRecordTypes +
                    ". Supported record types: " + Arrays.toString(WALRecord.RecordType.values()));
            }
        }

        return recordTypes;
    }

    /**
     * Parse command line arguments and return filled IgniteWalConverterArguments
     *
     * @param args Command line arguments.
     * @return IgniteWalConverterArguments.
     */
    public static IgniteWalConverterArguments parse(final PrintStream out, String args[]) {
        AtomicReference<CLIArgumentParser> parserRef = new AtomicReference<>();

        CLIArgumentParser parser = new CLIArgumentParser(asList(
            optionalArg(WAL_DIR.arg(), "Path to dir with wal files.", String.class, () -> {
                if (parserRef.get().get(Args.WAL_ARCHIVE_DIR.arg()) == null)
                    throw new IllegalArgumentException("One of the arguments --wal-dir or --wal-archive-dir must be specified.");
                else
                    return null;
            }),
            optionalArg(Args.WAL_ARCHIVE_DIR.arg(), "Path to dir with wal files.", String.class),
            optionalArg(PAGE_SIZE.arg(), "Size of pages, which was selected for file store (1024, 2048, 4096, etc).", Integer.class, () -> 4096),
            optionalArg(BINARY_METADATA_DIR.arg(), "Path to binary meta.", String.class),
            optionalArg(MARSHALLER_MAPPING_DIR.arg(), "Path to marshaller dir.", String.class),
            optionalArg(UNWRAP_BINARY.arg(), "Unwrap binary non-primitive objects.", Boolean.class),
            optionalArg(RECORD_TYPES.arg(), "Comma-separated WAL record types (TX_RECORD, DATA_RECORD, etc). By default, all types will be printed.", String[].class),
            optionalArg(WAL_TIME_FROM_MILLIS.arg(), "The start time interval for the record time in milliseconds.", Long.class),
            optionalArg(WAL_TIME_TO_MILLIS.arg(), "The end time interval for the record time in milliseconds.", Long.class),
            optionalArg(HAS_TEXT.arg(), "Filter by substring in the WAL record.", String.class),
            optionalArg(INCLUDE_SENSITIVE.arg(), "Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5). Default MD5.", String.class, ProcessSensitiveData.MD5::toString),
            optionalArg(PRINT_STAT.arg(), "Write summary statistics for WAL.", Boolean.class),
            optionalArg(SKIP_CRC.arg(), "Skip CRC calculation/check flag", Boolean.class)
        ));

        if (args == null || args.length < 1) {
            out.println("Print WAL log data in human-readable form.");
            out.println(parser.usage());
            out.println("For example:");
            out.println("    " + WAL_DIR.arg() + " /work/db/wal");
            out.println("    " + WAL_ARCHIVE_DIR.arg() + " /work/db/wal_archive");
            out.println("    " + PAGE_SIZE.arg() + " 4096");
            out.println("    " + BINARY_METADATA_DIR.arg() + " /work/db/nodeId-consistentId");
            out.println("    " + MARSHALLER_MAPPING_DIR.arg() + " /work/db/marshaller");
            out.println("    " + UNWRAP_BINARY.arg());
            out.println("    " + RECORD_TYPES.arg() + " DataRecord,TxRecord");
            out.println("    " + WAL_TIME_FROM_MILLIS.arg() + " 1575158400000");
            out.println("    " + WAL_TIME_TO_MILLIS.arg() + " 1577836740999");
            out.println("    " + HAS_TEXT.arg() + " search_string");
            out.println("    " + INCLUDE_SENSITIVE.arg() + " SHOW");
            out.println("    " + SKIP_CRC.arg());

            return null;
        }

        parserRef.set(parser);

        parser.parse(asList(args).iterator());

        File walDir = file(parser.get(WAL_DIR.arg()), true, false);
        File walArchiveDir = file(parser.get(Args.WAL_ARCHIVE_DIR.arg()), true, false);
        int pageSize = parser.get(PAGE_SIZE.arg());
        File binaryMetadataDir = file(parser.get(BINARY_METADATA_DIR.arg()), true, true);
        File marshallerMappingDir = file(parser.get(MARSHALLER_MAPPING_DIR.arg()), true, true);
        boolean unwrapBinary = parser.get(UNWRAP_BINARY.arg());
        final Set<WALRecord.RecordType> recordTypes = checkRecordTypes(parser.get(RECORD_TYPES.arg()));
        Long fromTime = parser.get(Args.WAL_TIME_FROM_MILLIS.arg());
        Long toTime = parser.get(Args.WAL_TIME_TO_MILLIS.arg());
        String hasText = parser.get(Args.HAS_TEXT.arg());
        boolean printStat = parser.get(Args.PRINT_STAT.arg());
        boolean skipCrc = parser.get(Args.SKIP_CRC.arg());

        String processSensitiveDataStr = parser.get(Args.INCLUDE_SENSITIVE.arg());

        ProcessSensitiveData includeSensitive;

        try {
            includeSensitive = ProcessSensitiveData.valueOf(processSensitiveDataStr);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Unknown --include-sensitive: " + processSensitiveDataStr +
                ". Supported: " + Arrays.toString(ProcessSensitiveData.values()));
        }

        out.println("Program arguments:");

        if (walDir != null)
            out.printf("\t%s = %s\n", WAL_DIR, walDir.getAbsolutePath());

        if (walArchiveDir != null)
            out.printf("\t%s = %s\n", WAL_ARCHIVE_DIR, walArchiveDir.getAbsolutePath());

        out.printf("\t%s = %d\n", PAGE_SIZE, pageSize);

        if (binaryMetadataDir != null)
            out.printf("\t%s = %s\n", BINARY_METADATA_DIR, binaryMetadataDir);

        if (marshallerMappingDir != null)
            out.printf("\t%s = %s\n", MARSHALLER_MAPPING_DIR, marshallerMappingDir);

        out.printf("\t%s = %s\n", UNWRAP_BINARY, unwrapBinary);

        if (!F.isEmpty(recordTypes))
            out.printf("\t%s = %s\n", RECORD_TYPES, recordTypes);

        if (fromTime != null)
            out.printf("\t%s = %s\n", WAL_TIME_FROM_MILLIS, new Date(fromTime));

        if (toTime != null)
            out.printf("\t%s = %s\n", WAL_TIME_TO_MILLIS, new Date(toTime));

        if (hasText != null)
            out.printf("\t%s = %s\n", HAS_TEXT, hasText);

        out.printf("\t%s = %s\n", INCLUDE_SENSITIVE, includeSensitive);

        out.printf("\t%s = %b\n", PRINT_STAT, printStat);

        out.printf("\t%s = %b\n", SKIP_CRC, skipCrc);

        return new IgniteWalConverterArguments(walDir, walArchiveDir, pageSize,
            binaryMetadataDir, marshallerMappingDir,
            unwrapBinary, recordTypes, fromTime, toTime, hasText, includeSensitive, printStat, skipCrc);
    }

    /**
     * WAL converter arguments.
     */
    public enum Args {
        /** */
        WAL_DIR("--wal-dir"),
        /** */
        WAL_ARCHIVE_DIR("--wal-archive-dir"),
        /** */
        PAGE_SIZE("--page-size"),
        /** */
        BINARY_METADATA_DIR("--binary-metadata-dir"),
        /** */
        MARSHALLER_MAPPING_DIR("--marshaller-mapping-dir"),
        /** */
        UNWRAP_BINARY("--unwrap-binary"),
        /** */
        RECORD_TYPES("--record-types"),
        /** */
        WAL_TIME_FROM_MILLIS("--wal-time-from-millis"),
        /** */
        WAL_TIME_TO_MILLIS("--wal-time-to-millis"),
        /** */
        HAS_TEXT("--has-text"),
        /** */
        INCLUDE_SENSITIVE("--include-sensitive"),
        /** */
        PRINT_STAT("--print-stat"),
        /** */
        SKIP_CRC("--skip-crc");

        /** */
        private String arg;

        /** */
        Args(String arg) {
            this.arg = arg;
        }

        /** */
        public String arg() {
            return arg;
        }
    }
}
