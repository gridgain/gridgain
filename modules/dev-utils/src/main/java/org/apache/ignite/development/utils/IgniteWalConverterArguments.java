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

package org.apache.ignite.development.utils;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Parameters for IgniteWalConverter with parsed and validated.
 */
public class IgniteWalConverterArguments {
    /** */
    private static final String WAL_DIR = "walDir";

    /** */
    private static final String WAL_ARCHIVE_DIR = "walArchiveDir";

    /** */
    private static final String PAGE_SIZE = "pageSize";

    /** */
    private static final String BINARY_METADATA_DIR = "binaryMetadataDir";

    /** */
    private static final String MARSHALLER_MAPPING_DIR = "marshallerMappingDir";

    /** */
    private static final String KEEP_BINARY = "keepBinary";

    /** */
    private static final String RECORD_TYPES = "recordTypes";

    /** */
    private static final String WAL_TIME_FROM_MILLIS = "walTimeFromMillis";

    /** */
    private static final String WAL_TIME_TO_MILLIS = "walTimeToMillis";

    /** */
    private static final String HAS_TEXT = "hasText";

    /** */
    private static final String INCLUDE_SENSITIVE = "includeSensitive";

    /** */
    private static final String PRINT_STAT = "printStat";

    /** */
    private static final String SKIP_CRC = "skipCrc";

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
    private final boolean keepBinary;

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
     * @param keepBinary                    Keep binary flag.
     * @param recordTypes                   WAL record types (TX_RECORD, DATA_RECORD, etc).
     * @param fromTime                      The start time interval for the record time in milliseconds.
     * @param toTime                        The end time interval for the record time in milliseconds.
     * @param hasText                       Filter by substring in the WAL record.
     * @param includeSensitive              Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5).
     * @param printStat                     Write summary statistics for WAL.
     * @param skipCrc                       Skip CRC calculation/check flag.
     */
    public IgniteWalConverterArguments(File walDir, File walArchiveDir, int pageSize,
        File binaryMetadataDir, File marshallerMappingDir, boolean keepBinary,
        Set<WALRecord.RecordType> recordTypes, Long fromTime, Long toTime, String hasText,
        ProcessSensitiveData includeSensitive,
        boolean printStat, boolean skipCrc) {
        this.walDir = walDir;
        this.walArchiveDir = walArchiveDir;
        this.pageSize = pageSize;
        this.binaryMetadataDir = binaryMetadataDir;
        this.marshallerMappingDir = marshallerMappingDir;
        this.keepBinary = keepBinary;
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
    public boolean isKeepBinary() {
        return keepBinary;
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
     * Parse command line arguments and return filled IgniteWalConverterArguments
     *
     * @param args Command line arguments.
     * @return IgniteWalConverterArguments.
     */
    public static IgniteWalConverterArguments parse(final PrintStream out, String args[]) {
        if (args == null || args.length < 1) {
            out.println("Print WAL log data in human-readable form.");
            out.println("You need to provide:");
            out.println("    walDir                           Path to dir with wal files.");
            out.println("    walArchiveDir                    Path to dir with archive wal files. walDir or walArchiveDir must be specified.");
            out.println("    pageSize                         Size of pages, which was selected for file store (1024, 2048, 4096, etc). Default 4096.");
            out.println("    binaryMetadataDir                (Optional) Path to binary meta.");
            out.println("    marshallerMappingDir             (Optional) Path to marshaller dir.");
            out.println("    keepBinary                       Keep binary flag. Default true.");
            out.println("    recordTypes                      (Optional) Comma-separated WAL record types (TX_RECORD, DATA_RECORD, etc). Default all.");
            out.println("    walTimeFromMillis                (Optional) The start time interval for the record time in milliseconds.");
            out.println("    walTimeToMillis                  (Optional) The end time interval for the record time in milliseconds.");
            out.println("    hasText                          (Optional) Filter by substring in the WAL record.");
            out.println("    includeSensitive                 (Optional) Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5). Default SHOW.");
            out.println("    printStat                        Write summary statistics for WAL. Default false.");
            out.println("    skipCrc                          Skip CRC calculation/check flag. Default false.");
            out.println("For example:");
            out.println("    walDir=/work/db/wal");
            out.println("    walArchiveDir=/work/db/wal_archive");
            out.println("    pageSize=4096");
            out.println("    binaryMetadataDir=/work/db/nodeId-consistentId");
            out.println("    marshallerMappingDir=/work/db/marshaller");
            out.println("    keepBinary=true");
            out.println("    recordTypes=DataRecord,TxRecord");
            out.println("    walTimeFromMillis=1575158400000");
            out.println("    walTimeToMillis=1577836740999");
            out.println("    hasText=search string");
            out.println("    includeSensitive=SHOW");
            out.println("    skipCrc=true");
            return null;
        }

        File walDir = null;
        File walArchiveDir = null;
        int pageSize = 4096;
        File binaryMetadataDir = null;
        File marshallerMappingDir = null;
        boolean keepBinary = true;
        final Set<WALRecord.RecordType> recordTypes = new HashSet<>();
        Long fromTime = null;
        Long toTime = null;
        String hasText = null;
        ProcessSensitiveData includeSensitive = ProcessSensitiveData.SHOW;
        boolean printStat = false;
        boolean skipCrc = false;

        for (String arg : args) {
            if (arg.startsWith(WAL_DIR + "=")) {
                final String walPath = arg.substring(WAL_DIR.length() + 1);

                walDir = new File(walPath);

                if (!walDir.exists())
                    throw new IllegalArgumentException("Incorrect path to dir with wal files: " + walPath);
            }
            else if (arg.startsWith(WAL_ARCHIVE_DIR + "=")) {
                final String walArchivePath = arg.substring(WAL_ARCHIVE_DIR.length() + 1);

                walArchiveDir = new File(walArchivePath);

                if (!walArchiveDir.exists())
                    throw new IllegalArgumentException("Incorrect path to dir with archive wal files: " + walArchivePath);
            }
            else if (arg.startsWith(PAGE_SIZE + "=")) {
                final String pageSizeStr = arg.substring(PAGE_SIZE.length() + 1);

                try {
                    pageSize = Integer.parseInt(pageSizeStr);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Incorrect page size. Error parse: " + pageSizeStr);
                }
            }
            else if (arg.startsWith(BINARY_METADATA_DIR + "=")) {
                final String binaryMetadataPath = arg.substring(BINARY_METADATA_DIR.length() + 1);

                binaryMetadataDir = new File(binaryMetadataPath);

                if (!binaryMetadataDir.isDirectory())
                    throw new IllegalArgumentException("Incorrect path to dir with binary meta files: " + binaryMetadataPath);
            }
            else if (arg.startsWith(MARSHALLER_MAPPING_DIR + "=")) {
                final String marshallerMappingPath = arg.substring(MARSHALLER_MAPPING_DIR.length() + 1);

                marshallerMappingDir = new File(marshallerMappingPath);

                if (!marshallerMappingDir.isDirectory())
                    throw new IllegalArgumentException("Incorrect path to dir with marshaller files: " + marshallerMappingPath);
            }
            else if (arg.startsWith(KEEP_BINARY + "=")) {
                keepBinary = parseBoolean(KEEP_BINARY, arg.substring(KEEP_BINARY.length() + 1));
            }
            else if (arg.startsWith(RECORD_TYPES + "=")) {
                final String recordTypesStr = arg.substring(RECORD_TYPES.length() + 1);

                final String[] recordTypesStrArray = recordTypesStr.split(",");

                final SortedSet<String> unknownRecordTypes = new TreeSet<>();

                for (String recordTypeStr : recordTypesStrArray) {
                    try {
                        recordTypes.add(WALRecord.RecordType.valueOf(recordTypeStr));
                    }
                    catch (Exception e) {
                        unknownRecordTypes.add(recordTypeStr);
                    }
                }

                if (!unknownRecordTypes.isEmpty())
                    throw new IllegalArgumentException("Unknown record types: " + unknownRecordTypes +
                        ". Supported record types: " + Arrays.toString(WALRecord.RecordType.values()));
            }
            else if (arg.startsWith(WAL_TIME_FROM_MILLIS + "=")) {
                final String fromTimeStr = arg.substring(WAL_TIME_FROM_MILLIS.length() + 1);

                try {
                    fromTime = Long.parseLong(fromTimeStr);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Incorrect walTimeFromMillis. Error parse: " + fromTimeStr);
                }
            }
            else if (arg.startsWith(WAL_TIME_TO_MILLIS + "=")) {
                final String toTimeStr = arg.substring(WAL_TIME_TO_MILLIS.length() + 1);

                try {
                    toTime = Long.parseLong(toTimeStr);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Incorrect walTimeToMillis. Error parse: " + toTimeStr);
                }
            }
            else if (arg.startsWith(HAS_TEXT + "=")) {
                hasText = arg.substring(HAS_TEXT.length() + 1);
            }
            else if (arg.startsWith(INCLUDE_SENSITIVE + "=")) {
                final String includeSensitiveStr = arg.substring(INCLUDE_SENSITIVE.length() + 1);
                try {
                    includeSensitive = ProcessSensitiveData.valueOf(includeSensitiveStr);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Unknown includeSensitive: " + includeSensitiveStr +
                        ". Supported: " + Arrays.toString(ProcessSensitiveData.values()));
                }
            }
            else if (arg.startsWith(PRINT_STAT + "=")) {
                printStat = parseBoolean(PRINT_STAT, arg.substring(PRINT_STAT.length() + 1));
            }
            else if (arg.startsWith(SKIP_CRC + "=")) {
                skipCrc = parseBoolean(SKIP_CRC, arg.substring(SKIP_CRC.length() + 1));
            }
        }

        if (walDir == null && walArchiveDir == null)
            throw new IllegalArgumentException("The paths to the WAL files are not specified.");

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

        out.printf("\t%s = %s\n", KEEP_BINARY, keepBinary);

        if (!F.isEmpty(recordTypes))
            out.printf("\t%s = %s\n", RECORD_TYPES, recordTypes);

        if (fromTime != null)
            out.printf("\t%s = %s\n", WAL_TIME_FROM_MILLIS, new Date(fromTime));

        if (toTime != null)
            out.printf("\t%s = %s\n", WAL_TIME_TO_MILLIS, new Date(toTime));

        if (hasText != null)
            out.printf("\t%s = %s\n", HAS_TEXT, hasText);

        out.printf("\t%s = %b\n", PRINT_STAT, printStat);

        out.printf("\t%s = %b\n", SKIP_CRC, skipCrc);

        return new IgniteWalConverterArguments(walDir, walArchiveDir, pageSize,
            binaryMetadataDir, marshallerMappingDir,
            keepBinary, recordTypes, fromTime, toTime, hasText, includeSensitive, printStat, skipCrc);
    }

    /**
     * Parses the string argument as a boolean.  The {@code boolean}
     * returned represents the value {@code true} if the string argument
     * is not {@code null} and is equal, ignoring case, to the string
     * {@code "true"}, returned value {@code false} if the string argument
     * is not {@code null} and is equal, ignoring case, to the string
     * {@code "false"}, else throw IllegalArgumentException<p>
     *
     * @param name parameter name of boolean type.
     * @param value the {@code String} containing the boolean representation to be parsed.
     * @return the boolean represented by the string argument
     *
     */
    private static boolean parseBoolean(String name, String value) {
        if (value == null)
            throw new IllegalArgumentException("Null value passed for flag " + name);

        if (value.equalsIgnoreCase(Boolean.TRUE.toString()))
            return true;
        else if (value.equalsIgnoreCase(Boolean.FALSE.toString()))
            return false;
        else
            throw new IllegalArgumentException("Incorrect flag " + name + ", valid value: true or false. Error parse: " + value);
    }
}
