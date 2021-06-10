package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

public class WalFinder1 {
    public static JdkMarshaller marshaller = new JdkMarshaller();

//    public static final String walDir1 = "/home/sutsel/work/tickets/GG-25687/test7/work/db/wal/nodetransactions_TxPartitionCounterStateConsistencyTest0";
//    public static final String walDir2 = "/home/sutsel/work/tickets/GG-25687/test7/work/db/wal/nodetransactions_TxPartitionCounterStateConsistencyTest1";
    public static final String walDir1 = "/home/sutsel/work/gitrepo/gridgain/gridgain/incubator-ignite/work/db/wal/nodetransactions_TxPartitionCounterStateConsistencyTest0";
    public static final String walDir2 = "/home/sutsel/work/gitrepo/gridgain/gridgain/incubator-ignite/work/db/wal/nodetransactions_TxPartitionCounterStateConsistencyTest1";

    public static final int grpId = CU.cacheId("CACHEGROUP_DICTIONARY");
    public static final int partId = 1;
//    public static final long keyId= 60;

    // [topVer=151936057, order=1540460670614, nodeOrder=5]
//    public static GridCacheVersion xidVersion = new GridCacheVersion(151936057, 1540460670614L, 5, 0);

    public static Map<Long, List<DataEntry>> updCntrToDataEntry = new HashMap<>();
    public static List<IgniteBiTuple<WALPointer, DataEntry>> history = new ArrayList<>();
    public static List<IgniteBiTuple<WALPointer, WALRecord>> txHistory = new ArrayList<>();

    public static void main(String[] args) {
        System.setProperty(IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING, "plain");

        // initialize mapping
        for (String cacheName : cacheNames)
            cacheIdToCacheName.put(CU.cacheId(cacheName), cacheName);

        parseWalFiles(walDir1);
        parseWalFiles(walDir2);
    }

    static void parseWalFiles(String walPath) {
        history.clear();

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder();

        params.bufferSize(2 * 1024 * 1024);
        params.filesOrDirs(walPath);
        params.filter(new WalRecordFilter());
        //params.from(new FileWALPointer(112L, 12627438, 191));

        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println("Start parsing wal files from " + walPath + " ...");

        try (WALIterator itr = new IgniteWalIteratorFactory().iterator(params)) {
            while (itr.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> walEntry = itr.next();

//                if (walEntry.get2() instanceof MetastoreDataRecord)
//                    processMetastoreDataRecord((MetastoreDataRecord)walEntry.get2());
//                else
                if (walEntry.get2() instanceof DataRecord)
                    processDataRecord(walEntry.get1(), (DataRecord)walEntry.get2());
            }
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
        finally {
        }

        System.out.println("\nThe following entries have been found: ");
        for (IgniteBiTuple<WALPointer, DataEntry> d : history)
            System.out.println(d.get1() + ", " + d.get2());

        System.out.println("-----------------------------------------------------------------------------------------");
    }

//    static void parseWalFilesTx(String walPath) {
//        txHistory.clear();
//
//        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder();
//
//        params.bufferSize(2 * 1024 * 1024);
//        params.filesOrDirs(walPath);
//        params.filter(new WalTxRecordFilter());
//
//        System.out.println("-----------------------------------------------------------------------------------------");
//        System.out.println("Start parsing wal files from " + walPath + " ...");
//
//        try (WALIterator itr = new IgniteWalIteratorFactory().iterator(params)) {
//            while (itr.hasNext()) {
//                IgniteBiTuple<WALPointer, WALRecord> walEntry = itr.next();
//
//                if (walEntry.get2() instanceof TxRecord)
//                    processTxRecord(walEntry.get1(), walEntry.get2());
//                else if (walEntry.get2() instanceof DataRecord)
//                    processTxDataRecord(walEntry.get1(), walEntry.get2());
//            }
//        }
//        catch (IgniteCheckedException e) {
//            e.printStackTrace();
//        }
//        finally {
//        }
//
//        System.out.println("\nThe following entries have been found: ");
//        for (IgniteBiTuple<WALPointer, WALRecord> d : txHistory)
//            System.out.println(d.get1() + ", " + d.get2());
//
//        System.out.println("-----------------------------------------------------------------------------------------");
//    }

//    static void processTxRecord(WALPointer ptr, WALRecord wr) throws IgniteCheckedException {
//        TxRecord rec = (TxRecord)wr;
//        if (rec.nearXidVersion().equals(xidVersion))
//            txHistory.add(new IgniteBiTuple<>(rec.position(), rec));
//    }

//    static void processTxDataRecord(WALPointer ptr, WALRecord wr) throws IgniteCheckedException {
//        DataRecord rec = (DataRecord)wr;
//        for (DataEntry entry : rec.writeEntries()) {
//            if (/*entry.partitionId() == partId && cacheIdToCacheName.containsKey(entry.cacheId()) &&*/ entry.nearXidVersion().equals(xidVersion)) {
//                txHistory.add(new IgniteBiTuple<>(wr.position(), wr));
//
//                break;
//            }
//        }
//    }

    static void processDataRecord(WALPointer ptr, DataRecord rec) throws IgniteCheckedException {
        for (DataEntry entry : rec.writeEntries()) {
            if (entry.partitionId() == partId && cacheIdToCacheName.containsKey(entry.cacheId()))
                history.add(new IgniteBiTuple<>(rec.position(), entry));
        }
    }

    public static class WalRecordFilter implements IgniteBiPredicate<WALRecord.RecordType, WALPointer> {
        @Override public boolean apply(WALRecord.RecordType type, WALPointer pointer) {
            return type == WALRecord.RecordType.DATA_RECORD /*||
                type == WALRecord.RecordType.METASTORE_DATA_RECORD*/;
        }
    }

    public static class WalTxRecordFilter implements IgniteBiPredicate<WALRecord.RecordType, WALPointer> {
        @Override public boolean apply(WALRecord.RecordType type, WALPointer pointer) {
            return type == WALRecord.RecordType.DATA_RECORD ||
                type == WALRecord.RecordType.TX_RECORD;
        }
    }

    public static Map<Integer, String> cacheIdToCacheName = new HashMap<>();

    public static String[] cacheNames = {
        "default"
    };
}
