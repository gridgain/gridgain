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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;
import static org.apache.ignite.IgniteSystemProperties.getEnum;
import static org.apache.ignite.development.utils.ProcessSensitiveData.HIDE;
import static org.apache.ignite.development.utils.ProcessSensitiveData.SHOW;

/**
 * Print WAL log data in human-readable form.
 */
public class IgniteWalConverter {
    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            throw new IllegalArgumentException("\nYou need to provide:\n" +
                "\t1. Size of pages, which was selected for file store (1024, 2048, 4096, etc).\n" +
                "\t2. Path to dir with wal files.\n" +
                "\t3. (Optional) Path to dir with archive wal files.");

        final int pageSize = Integer.parseInt(args[0]);
        final String walPath = args[1];
        final String walArchivePath = (args.length > 2) ? args[2] : null;

        if (F.isEmpty(walPath) && F.isEmpty(walArchivePath))
            throw new IllegalArgumentException("The paths to the WAL files are not specified.");

        File walDir = checkFile(walPath, "wal");

        File walArchiveDir = checkFile(walArchivePath, "archive wal");

        convert(System.out, pageSize,
            walDir, walArchiveDir,
            null, null,
            true, true);
    }

    /**
     * Create File by path if path not empty and check exists.
     *
     * @param path        Path.
     * @param description Description path for error message.
     * @return File.
     */
    private static File checkFile(String path, String description) {
        File file = null;

        if (!F.isEmpty(path)) {
            file = new File(path);

            if (!file.exists())
                throw new IllegalArgumentException("Incorrect path to dir with " + description + " files: " +
                    path + "(" + file.getAbsolutePath() + ")");
        }

        return file;
    }

    /**
     * Write to out WAL log data in human-readable form.
     *
     * @param out                           Receiver of result.
     * @param pageSize                      Size of pages, which was selected for file store (1024, 2048, 4096, etc).
     * @param walDir                        Path to dir with wal files.
     * @param walArchiveDir                 Path to dir with archive wal files.
     * @param binaryMetadataFileStoreDir    Path to binary metadata dir.
     * @param marshallerMappingFileStoreDir Path to marshaller dir.
     * @param keepBinary                    Keep binary flag.
     * @throws IgniteCheckedException If failed.
     */
    public static void convert(
        final PrintStream out,
        final int pageSize,
        final File walDir,
        final File walArchiveDir,
        final File binaryMetadataFileStoreDir,
        final File marshallerMappingFileStoreDir,
        final boolean keepBinary,
        final boolean skipCrc
    ) throws IgniteCheckedException {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS, H2MvccInnerIO.VERSIONS, H2MvccLeafIO.VERSIONS);
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();

        System.setProperty(IgniteSystemProperties.IGNITE_PDS_SKIP_CRC, Boolean.toString(skipCrc));
        RecordV1Serializer.skipCrc = skipCrc;
        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH, String.valueOf(Integer.MAX_VALUE));

        boolean printRecords = IgniteSystemProperties.getBoolean("PRINT_RECORDS", false); //TODO read them from argumetns
        boolean printStat = IgniteSystemProperties.getBoolean("PRINT_STAT", true); //TODO read them from argumetns
        ProcessSensitiveData sensitiveData = getEnum("SENSITIVE_DATA", SHOW); //TODO read them from argumetns

        if (printRecords && HIDE == sensitiveData)
            System.setProperty(IGNITE_TO_STRING_INCLUDE_SENSITIVE, Boolean.FALSE.toString());

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger());

        @Nullable final WalStat stat = printStat ? new WalStat() : null;

        IgniteWalIteratorFactory.IteratorParametersBuilder iteratorParametersBuilder = new IgniteWalIteratorFactory.IteratorParametersBuilder().
            pageSize(pageSize).
            binaryMetadataFileStoreDir(binaryMetadataFileStoreDir).
            marshallerMappingFileStoreDir(marshallerMappingFileStoreDir).
            keepBinary(keepBinary);

        if (walDir != null)
            iteratorParametersBuilder.filesOrDirs(walDir);

        if (walArchiveDir != null)
            iteratorParametersBuilder.filesOrDirs(walArchiveDir);

        try (WALIterator stIt = factory.iterator(iteratorParametersBuilder)) {
            String currentWalPath = null;

            while (stIt.hasNextX()) {
                final String currentRecordWalPath = getCurrentWalFilePath(stIt);

                if (currentWalPath == null || !currentWalPath.equals(currentRecordWalPath)) {
                    out.println("File: " + currentRecordWalPath);

                    currentWalPath = currentRecordWalPath;
                }

                IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();

                final WALPointer pointer = next.get1();

                final WALRecord record = next.get2();

                if (stat != null)
                    stat.registerRecord(record, pointer, true);

                out.println(toString(record, sensitiveData));
            }
        }
        catch (Exception e) {
            e.printStackTrace(out);
        }

        if (stat != null)
            out.println("Statistic collected:\n" + stat.toString());
    }

    /**
     * Get current wal file path, used in {@code WALIterator}
     *
     * @param it WALIterator.
     * @return Current wal file path.
     */
    private static String getCurrentWalFilePath(WALIterator it) {
        String result = null;
        try {
            final Integer curIdx = (Integer)getFieldValue(it, "curIdx");

            final List<FileDescriptor> walFileDescriptors = (List)getFieldValue(it, "walFileDescriptors");

            if (curIdx != null && walFileDescriptors != null && !walFileDescriptors.isEmpty())
                result = walFileDescriptors.get(curIdx).getAbsolutePath();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Find field in class by name
     *
     * @param c         Class.
     * @param fieldName Field name.
     * @return Field.
     */
    public static Field getField(Class<?> c, String fieldName) {
        Field result = null;
        while (c != null && result == null) {
            try {
                result = c.getDeclaredField(fieldName);
            }
            catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }
        return result;
    }

    /**
     * Get a field Value in object by field name
     *
     * @param o         Object.
     * @param fieldName Field name.
     * @return Field value.
     * @throws IllegalAccessException If failed.
     */
    public static Object getFieldValue(Object o, String fieldName) throws IllegalAccessException {
        Object result = null;

        Field field = getField(o.getClass(), fieldName);

        if (field != null) {
            field.setAccessible(true);

            result = field.get(o);
        }
        return result;
    }

    /**
     * Converting {@link WALRecord} to a string with sensitive data.
     *
     * @param walRecord     Instance of {@link WALRecord}.
     * @param sensitiveData Strategy for processing of sensitive data.
     * @return String representation of {@link WALRecord}.
     */
    private static String toString(WALRecord walRecord, ProcessSensitiveData sensitiveData) {
        if (walRecord instanceof DataRecord) {
            final DataRecord dataRecord = (DataRecord)walRecord;

            final List<DataEntry> entryWrappers = new ArrayList<>(dataRecord.writeEntries().size());

            for (DataEntry dataEntry : dataRecord.writeEntries())
                entryWrappers.add(new DataEntryWrapper(dataEntry, sensitiveData));

            dataRecord.setWriteEntries(entryWrappers);
        }
        else if (SHOW == sensitiveData || HIDE == sensitiveData)
            return walRecord.toString();
        else if (walRecord instanceof MetastoreDataRecord)
            walRecord = new MetastoreDataRecordWrapper((MetastoreDataRecord)walRecord, sensitiveData);

        return walRecord.toString();
    }
}
