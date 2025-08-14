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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.TimeStampRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.FilteredWalIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.checkpoint;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.pageOwner;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters.partitionMetaStateUpdate;

/**
 * Print WAL log data in human-readable form.
 */
public class IgniteWalConverter {
    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) {
        final IgniteWalConverterArguments parameters = IgniteWalConverterArguments.parse(System.out, args);

        if (parameters != null)
            convert(System.out, parameters);
    }

    /**
     * Write to out WAL log data in human-readable form.
     *
     * @param out        Receiver of result.
     * @param params Parameters.
     */
    public static void convert(final PrintStream out, final IgniteWalConverterArguments params) {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS, H2MvccInnerIO.VERSIONS, H2MvccLeafIO.VERSIONS);
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();

        if (params.includeSensitive() == null)
            System.setProperty(IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING, "hash");
        else {
            switch (params.includeSensitive()) {
                case SHOW:
                    System.setProperty(IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING, "plain");
                    break;
                case HASH:
                case MD5:
                    System.setProperty(IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING, "hash");
                    break;
                case HIDE:
                    System.setProperty(IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING, "none");
                    break;
                default:
                    assert false : "Unexpected includeSensitive: " + params.includeSensitive();
            }
        }

        System.setProperty(IgniteSystemProperties.IGNITE_PDS_SKIP_CRC, Boolean.toString(params.isSkipCrc()));
        RecordV1Serializer.skipCrc = params.isSkipCrc();

        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH, String.valueOf(Integer.MAX_VALUE));

        final WalStat stat = params.isPrintStat() ? new WalStat() : null;

        IgniteWalIteratorFactory.IteratorParametersBuilder iteratorParametersBuilder = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .pageSize(params.getPageSize())
            .binaryMetadataFileStoreDir(params.getBinaryMetadataDir())
            .marshallerMappingFileStoreDir(params.getMarshallerMappingDir())
            .keepBinary(!params.isUnwrapBinary());

        if (params.getWalDir() != null)
            iteratorParametersBuilder.filesOrDirs(params.getWalDir());

        if (params.getWalArchiveDir() != null)
            iteratorParametersBuilder.filesOrDirs(params.getWalArchiveDir());

        IgniteWalIteratorFactoryIgnoreError factory = new IgniteWalIteratorFactoryIgnoreError();

        boolean printAlways = F.isEmpty(params.getRecordTypes());

        try (WALIterator stIt = walIterator(factory.iterator(iteratorParametersBuilder), params.getPages())) {
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

                if (printAlways || params.getRecordTypes().contains(record.type())) {
                    boolean print = true;

                    if (record instanceof TimeStampRecord)
                        print = withinTimeRange((TimeStampRecord) record, params.getFromTime(), params.getToTime());

                    final String recordStr = toString(record, params.includeSensitive());

                    if (print && (F.isEmpty(params.hasText()) || recordStr.contains(params.hasText())))
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
     * Checks if provided TimeStampRecord is within time range.
     *
     * @param rec Record.
     * @param fromTime Lower bound for timestamp.
     * @param toTime Upper bound for timestamp;
     * @return {@code True} if timestamp is within range.
     */
    private static boolean withinTimeRange(TimeStampRecord rec, Long fromTime, Long toTime) {
        if (fromTime != null && rec.timestamp() < fromTime)
            return false;

        if (toTime != null && rec.timestamp() > toTime)
            return false;

        return true;
    }

    /**
     * Get current wal file path, used in {@code WALIterator}.
     *
     * @param it WALIterator.
     * @return Current wal file path.
     */
    private static String getCurrentWalFilePath(WALIterator it) {
        String res = null;

        try {
            WALIterator walIter = it instanceof FilteredWalIterator ? U.field(it, "delegateWalIter") : it;

            Integer curIdx = U.field(walIter, "curIdx");

            List<FileDescriptor> walFileDescriptors = U.field(walIter, "walFileDescriptors");

            if (curIdx != null && walFileDescriptors != null && curIdx < walFileDescriptors.size())
                res = walFileDescriptors.get(curIdx).getAbsolutePath();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return res;
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
        else if (walRecord instanceof MetastoreDataRecord) {
            walRecord = new MetastoreDataRecordWrapper((MetastoreDataRecord) walRecord, sensitiveData);
        }
        else if (walRecord instanceof PageSnapshot) {
            return new PageSnapshotRecordWrapper((PageSnapshot) walRecord, TARGET_IOS).toString();
        }

        return walRecord.toString();
    }

    /**
     * Getting WAL iterator.
     *
     * @param walIter WAL iterator.
     * @param pageIds Pages for searching in format grpId:pageId.
     * @return WAL iterator.
     */
    private static WALIterator walIterator(
        WALIterator walIter,
        Collection<T2<Integer, Long>> pageIds
    ) throws IgniteCheckedException {
        Predicate<IgniteBiTuple<WALPointer, WALRecord>> filter = null;

        if (!pageIds.isEmpty()) {
            Set<T2<Integer, Long>> grpAndPageIds0 = new HashSet<>(pageIds);

            // Collect all (group, partition) partition pairs.
            Set<T2<Integer, Integer>> grpAndParts = grpAndPageIds0.stream()
                .map((tup) -> new T2<>(tup.get1(), PageIdUtils.partId(tup.get2())))
                .collect(Collectors.toSet());

            // Build WAL filter. (Checkoint, Page, Partition meta)
            filter = checkpoint().or(pageOwner(grpAndPageIds0)).or(partitionMetaStateUpdate(grpAndParts));
        }

        return filter != null ? new FilteredWalIterator(walIter, filter) : walIter;
    }

    private static final Map<String, Integer> IO_TYPES_MAP = new HashMap<>();

    static {
        // Types taken from org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO
        IO_TYPES_MAP.put("T_DATA", (int) PageIO.T_DATA);
        IO_TYPES_MAP.put("T_BPLUS_META", (int) PageIO.T_BPLUS_META);
        IO_TYPES_MAP.put("T_H2_REF_LEAF", (int) PageIO.T_H2_REF_LEAF);
        IO_TYPES_MAP.put("T_H2_REF_INNER", (int) PageIO.T_H2_REF_INNER);
        IO_TYPES_MAP.put("T_DATA_REF_INNER", (int) PageIO.T_DATA_REF_INNER);
        IO_TYPES_MAP.put("T_DATA_REF_LEAF", (int) PageIO.T_DATA_REF_LEAF);
        IO_TYPES_MAP.put("T_METASTORE_INNER", (int) PageIO.T_METASTORE_INNER);
        IO_TYPES_MAP.put("T_METASTORE_LEAF", (int) PageIO.T_METASTORE_LEAF);
        IO_TYPES_MAP.put("T_PENDING_REF_INNER", (int) PageIO.T_PENDING_REF_INNER);
        IO_TYPES_MAP.put("T_PENDING_REF_LEAF", (int) PageIO.T_PENDING_REF_LEAF);
        IO_TYPES_MAP.put("T_META", (int) PageIO.T_META);
        IO_TYPES_MAP.put("T_PAGE_LIST_META", (int) PageIO.T_PAGE_LIST_META);
        IO_TYPES_MAP.put("T_PAGE_LIST_NODE", (int) PageIO.T_PAGE_LIST_NODE);
        IO_TYPES_MAP.put("T_PART_META", (int) PageIO.T_PART_META);
        IO_TYPES_MAP.put("T_PAGE_UPDATE_TRACKING", (int) PageIO.T_PAGE_UPDATE_TRACKING);
        IO_TYPES_MAP.put("T_CACHE_ID_AWARE_DATA_REF_INNER", (int) PageIO.T_CACHE_ID_AWARE_DATA_REF_INNER);
        IO_TYPES_MAP.put("T_CACHE_ID_AWARE_DATA_REF_LEAF", (int) PageIO.T_CACHE_ID_AWARE_DATA_REF_LEAF);
        IO_TYPES_MAP.put("T_CACHE_ID_AWARE_PENDING_REF_INNER", (int) PageIO.T_CACHE_ID_AWARE_PENDING_REF_INNER);
        IO_TYPES_MAP.put("T_CACHE_ID_AWARE_PENDING_REF_LEAF", (int) PageIO.T_CACHE_ID_AWARE_PENDING_REF_LEAF);
        IO_TYPES_MAP.put("T_PART_CNTRS", (int) PageIO.T_PART_CNTRS);
        IO_TYPES_MAP.put("T_DATA_METASTORAGE", (int) PageIO.T_DATA_METASTORAGE);
        IO_TYPES_MAP.put("T_DATA_REF_METASTORAGE_INNER", (int) PageIO.T_DATA_REF_METASTORAGE_INNER);
        IO_TYPES_MAP.put("T_DATA_REF_METASTORAGE_LEAF", (int) PageIO.T_DATA_REF_METASTORAGE_LEAF);
        IO_TYPES_MAP.put("T_DATA_REF_MVCC_INNER", (int) PageIO.T_DATA_REF_MVCC_INNER);
        IO_TYPES_MAP.put("T_DATA_REF_MVCC_LEAF", (int) PageIO.T_DATA_REF_MVCC_LEAF);
        IO_TYPES_MAP.put("T_CACHE_ID_DATA_REF_MVCC_INNER", (int) PageIO.T_CACHE_ID_DATA_REF_MVCC_INNER);
        IO_TYPES_MAP.put("T_CACHE_ID_DATA_REF_MVCC_LEAF", (int) PageIO.T_CACHE_ID_DATA_REF_MVCC_LEAF);
        IO_TYPES_MAP.put("T_H2_MVCC_REF_LEAF", (int) PageIO.T_H2_MVCC_REF_LEAF);
        IO_TYPES_MAP.put("T_H2_MVCC_REF_INNER", (int) PageIO.T_H2_MVCC_REF_INNER);
        IO_TYPES_MAP.put("T_TX_LOG_LEAF", (int) PageIO.T_TX_LOG_LEAF);
        IO_TYPES_MAP.put("T_TX_LOG_INNER", (int) PageIO.T_TX_LOG_INNER);
        IO_TYPES_MAP.put("T_DATA_PART", (int) PageIO.T_DATA_PART);
        IO_TYPES_MAP.put("T_MARKER_PAGE", (int) PageIO.T_MARKER_PAGE);
        IO_TYPES_MAP.put("T_DEFRAG_LINK_MAPPING_INNER", (int) PageIO.T_DEFRAG_LINK_MAPPING_INNER);
        IO_TYPES_MAP.put("T_DEFRAG_LINK_MAPPING_LEAF", (int) PageIO.T_DEFRAG_LINK_MAPPING_LEAF);
        IO_TYPES_MAP.put("T_H2_EX_REF_LEAF_START", (int) PageIO.T_H2_EX_REF_LEAF_START);
        IO_TYPES_MAP.put("T_H2_EX_REF_LEAF_END", (int) PageIO.T_H2_EX_REF_LEAF_END);
        IO_TYPES_MAP.put("T_H2_EX_REF_INNER_START", (int) PageIO.T_H2_EX_REF_INNER_START);
        IO_TYPES_MAP.put("T_H2_EX_REF_INNER_END", (int) PageIO.T_H2_EX_REF_INNER_END);
        IO_TYPES_MAP.put("T_H2_EX_REF_MVCC_LEAF_START", (int) PageIO.T_H2_EX_REF_MVCC_LEAF_START);
        IO_TYPES_MAP.put("T_H2_EX_REF_MVCC_LEAF_END", (int) PageIO.T_H2_EX_REF_MVCC_LEAF_END);
        IO_TYPES_MAP.put("T_H2_EX_REF_MVCC_INNER_START", (int) PageIO.T_H2_EX_REF_MVCC_INNER_START);
        IO_TYPES_MAP.put("T_H2_EX_REF_MVCC_INNER_END", (int) PageIO.T_H2_EX_REF_MVCC_INNER_END);

        // DR IOs
        IO_TYPES_MAP.put("T_UPDATE_LOG_REF_INNER", (int) PageIO.T_UPDATE_LOG_REF_INNER & 0xFFFF);
        IO_TYPES_MAP.put("T_UPDATE_LOG_REF_LEAF", (int) PageIO.T_UPDATE_LOG_REF_LEAF & 0xFFFF);
        IO_TYPES_MAP.put("T_CACHE_ID_AWARE_UPDATE_LOG_REF_INNER", (int) PageIO.T_CACHE_ID_AWARE_UPDATE_LOG_REF_INNER & 0xFFFF);
        IO_TYPES_MAP.put("T_CACHE_ID_AWARE_UPDATE_LOG_REF_LEAF", (int) PageIO.T_CACHE_ID_AWARE_UPDATE_LOG_REF_LEAF & 0xFFFF);
    }

    private static int[] parseTargetIoType(String csv) {
        String[] typeNames = csv.split(",");
        List<Integer> l = new ArrayList<>(typeNames.length);

        for (String typeName : typeNames) {
            Integer typeId = IO_TYPES_MAP.get(typeName);
            if (typeId != null) {
                l.add(typeId);
            }
        }

        return l.stream().mapToInt(Integer::intValue).toArray();
    }

    private static final String TARGET_IOS_STRING = IgniteSystemProperties.getString("TARGET_IOS_STRING", null);

    private static final int[] TARGET_IOS = TARGET_IOS_STRING != null
            ? parseTargetIoType(TARGET_IOS_STRING)
            : new int[]{};
}
