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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TimeStampRecord;
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
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Print WAL log data in human-readable form.
 */
public class IgniteWalConverter {
    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final IgniteWalConverterArguments parameters = IgniteWalConverterArguments.parse(System.out, args);

        if (parameters != null)
            convert(System.out, parameters);
    }

    /**
     * Write to out WAL log data in human-readable form.
     *
     * @param out        Receiver of result.
     * @param parameters Parameters.
     */
    public static void convert(final PrintStream out, final IgniteWalConverterArguments parameters) {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS, H2MvccInnerIO.VERSIONS, H2MvccLeafIO.VERSIONS);
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();

        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE,
            Boolean.toString(parameters.getProcessSensitiveData() == ProcessSensitiveData.HIDE));

        System.setProperty(IgniteSystemProperties.IGNITE_PDS_SKIP_CRC, Boolean.toString(parameters.isSkipCrc()));
        RecordV1Serializer.skipCrc = parameters.isSkipCrc();

        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH, String.valueOf(Integer.MAX_VALUE));

        final WalStat stat = parameters.isPrintStat() ? new WalStat() : null;

        IgniteWalIteratorFactory.IteratorParametersBuilder iteratorParametersBuilder = new IgniteWalIteratorFactory.IteratorParametersBuilder().
            pageSize(parameters.getPageSize()).
            binaryMetadataFileStoreDir(parameters.getBinaryMetadataFileStoreDir()).
            marshallerMappingFileStoreDir(parameters.getMarshallerMappingFileStoreDir()).
            keepBinary(parameters.isKeepBinary());

        if (parameters.getWalDir() != null)
            iteratorParametersBuilder.filesOrDirs(parameters.getWalDir());

        if (parameters.getWalArchiveDir() != null)
            iteratorParametersBuilder.filesOrDirs(parameters.getWalArchiveDir());

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory();

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

                if (F.isEmpty(parameters.getRecordTypes()) || parameters.getRecordTypes().contains(record.type())) {

                    boolean print = true;
                    if ((parameters.getFromTime() != null || parameters.getToTime() != null) && record instanceof TimeStampRecord) {
                        TimeStampRecord dataRecord = (TimeStampRecord)record;
                        if (parameters.getFromTime() != null && dataRecord.timestamp() < parameters.getFromTime())
                            print = false;

                        if (print && parameters.getToTime() != null && dataRecord.timestamp() > parameters.getToTime())
                            print = false;
                    }

                    final String recordStr = toString(record, parameters.getProcessSensitiveData());

                    if (print && (F.isEmpty(parameters.getRecordContainsText()) || recordStr.contains(parameters.getRecordContainsText())))
                        out.println(recordStr);
                }
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
            final Integer curIdx = IgniteUtils.field(it, "curIdx");

            final List<FileDescriptor> walFileDescriptors = IgniteUtils.field(it, "walFileDescriptors");

            if (curIdx != null && walFileDescriptors != null && !walFileDescriptors.isEmpty())
                result = walFileDescriptors.get(curIdx).getAbsolutePath();
        }
        catch (Exception e) {
            e.printStackTrace();
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
        else if (walRecord instanceof MetastoreDataRecord)
            walRecord = new MetastoreDataRecordWrapper((MetastoreDataRecord)walRecord, sensitiveData);

        return walRecord.toString();
    }
}
