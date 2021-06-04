package org.apache.ignite.internal.processors.cache.transactions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MVCC_DATA_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.junit.Assert.assertTrue;

public class WalFinder {

    //фильтрануть в wal: DATA_RECORD cache part operation.
    public static void main(String[] args) throws Exception{
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory();

        String workDir = U.defaultWorkDirectory();

//        String subfolderName = genNewStyleSubfolderName(0, new UUID(1, 2));

        File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);

//        IgniteWalIteratorFactory.IteratorParametersBuilder params =
//            createIteratorParametersBuilder(workDir, "nodetransactions_TxPartitionCounterStateConsistencyTest0")
//                .filesOrDirs(db);

        IgniteWalIteratorFactory.IteratorParametersBuilder params =
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filesOrDirs("/home/sutsel/work/tickets/GG-25687/test1/work/db/wal/nodetransactions_TxPartitionCounterStateConsistencyTest0");

        // Check iteratorArchiveDirectory and iteratorArchiveFiles are same.
        int cntArchiveDir = iterateAndCount(factory.iterator(params));

        System.out.println("Total records loaded using directory : " + cntArchiveDir);

        assertTrue(cntArchiveDir > 0);
    }

    /**
     * @param workDir Work directory.
     * @param subfolderName Subfolder name.
     * @return WAL iterator factory.
     * @throws IgniteCheckedException If failed.
     */
    @NotNull private static IgniteWalIteratorFactory.IteratorParametersBuilder createIteratorParametersBuilder(
        String workDir,
        String subfolderName
    ) throws IgniteCheckedException {
        File binaryMeta = U.resolveWorkDirectory(workDir, DataStorageConfiguration.DFLT_BINARY_METADATA_PATH,
            false);
//        File binaryMetaWithConsId = new File(binaryMeta, subfolderName);
        File marshallerMapping = U.resolveWorkDirectory(workDir, DataStorageConfiguration.DFLT_MARSHALLER_PATH, false);

        return new IgniteWalIteratorFactory.IteratorParametersBuilder()
//            .binaryMetadataFileStoreDir(binaryMetaWithConsId)
            .marshallerMappingFileStoreDir(marshallerMapping);
    }

    /**
     * Iterates on records and closes iterator.
     *
     * @param walIter iterator to count, will be closed.
     * @return count of records.
     * @throws IgniteCheckedException if failed to iterate.
     */
    private static int iterateAndCount(WALIterator walIter) throws IgniteCheckedException, IOException {
        int cnt = 0;

        BufferedWriter writer = new BufferedWriter(new FileWriter("/home/sutsel/work/tickets/GG-25687/test1/node0_wal.txt"));

        try (WALIterator it = walIter) {
            while (it.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

                WALRecord walRecord = tup.get2();

                if (walRecord.type() == DATA_RECORD || walRecord.type() == MVCC_DATA_RECORD) {
                    DataRecord record = (DataRecord)walRecord;

                    for (DataEntry entry : record.writeEntries()) {
                        KeyCacheObject key = entry.key();
                        CacheObject val = entry.value();

                        writer.write("Op: " + entry.op() + ", Key: " + key + ", Value: " + val);
                    }
                }

                writer.write("Record: " + walRecord);

                cnt++;
            }
        }

        writer.flush();

        writer.close();

        return cnt;
    }
}
