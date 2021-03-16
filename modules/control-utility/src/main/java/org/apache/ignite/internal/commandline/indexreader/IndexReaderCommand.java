package org.apache.ignite.internal.commandline.indexreader;

import static org.apache.ignite.internal.commandline.CommandList.INDEX_READER;

import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.File;
import java.io.PrintStream;
import java.util.Set;
import java.util.logging.Logger;

public class IndexReaderCommand extends AbstractCommand<IndexReaderCommand.Arguments> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
//        String CACHES = "cacheName1,...,cacheNameN";
//        String description = "Verify counters and hash sums of primary and backup partitions for the specified caches/cache " +
//                "groups on an idle cluster and print out the differences, if any. When no parameters are specified, " +
//                "all user caches are verified. Cache filtering options configure the set of caches that will be " +
//                "processed by " + IDLE_VERIFY + " command. If cache names are specified, in form of regular " +
//                "expressions, only matching caches will be verified. Caches matched by regexes specified after " +
//                EXCLUDE_CACHES + " parameter will be excluded from verification. Using parameter " + CACHE_FILTER +
//                " you can verify: only " + USER + " caches, only user " + PERSISTENT + " caches, only user " +
//                NOT_PERSISTENT + " caches, only " + SYSTEM + " caches, or " + ALL + " of the above.";
//
//        usageCache(
//                logger,
//                IDLE_VERIFY,
//                description,
//                Collections.singletonMap(CHECK_CRC.toString(),
//                        "check the CRC-sum of pages stored on disk before verifying data " +
//                                "consistency in partitions between primary and backup nodes."),
//                optional(DUMP), optional(SKIP_ZEROS), optional(CHECK_CRC), optional(EXCLUDE_CACHES, CACHES),
//                optional(CACHE_FILTER, or(ALL, USER, SYSTEM, PERSISTENT, NOT_PERSISTENT)), optional(CACHES));
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Partition directory. */
        private String dir;

        /** Full partitions count in cache group. */
        private int partCnt;

        /** Page size. */
        private int pageSize;

        /** Page store version. */
        private int pageStoreVer;

        /** Index tree names. */
        private Set<String> indexes;

        /**  File to print the report to. */
        private String destFile;

        /** Check cache data tree in partition files and it's consistency with indexes. */
        private boolean checkParts;

        public Arguments(String dir,
                         int partCnt,
                         int pageSize,
                         int pageStoreVer,
                         Set<String> indexes,
                         String destFile,
                         boolean checkParts) {
            this.dir = dir;
            this.partCnt = partCnt;
            this.pageSize = pageSize;
            this.pageStoreVer = pageStoreVer;
            this.indexes = indexes;
            this.destFile = destFile;
            this.checkParts = checkParts;
        }

        /** Partition directory. */
        public String dir() {
            return dir;
        }

        /** Full partitions count in cache group. */
        public int partCnt() {
            return partCnt;
        }

        /** Page size. */
        public int pageSize() {
            return pageSize;
        }

        /** Page store version. */
        public int pageStoreVer() {
            return pageStoreVer;
        }

        /** Index tree names. */
        public Set<String> indexes() {
            return indexes;
        }

        /** File to print the report to. */
        public String destFile() {
            return destFile;
        }

        /** Check cache data tree in partition files and it's consistency with indexes. */
        public boolean checkParts() {
            return checkParts;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexReaderCommand.Arguments.class, this);
        }
    }

    /** Command parsed arguments. */
    private IndexReaderCommand.Arguments args;

    /** {@inheritDoc} */
    @Override public IndexReaderCommand.Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        final IgniteIndexReaderFilePageStoreFactoryImpl factory = new IgniteIndexReaderFilePageStoreFactoryImpl(
                new File(args.dir()),
                args.pageSize(),
                args.partCnt(),
                args.pageStoreVer()
        );

        try (IgniteIndexReader idxReader = new IgniteIndexReader(
                idx -> args.indexes().contains(idx),
                args.checkParts(),
                new PrintStream(args.destFile()),
                factory
        )) {
            idxReader.readIdx();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String dir = "";
        int partCnt = 0;
        int pageSize = 4096;
        int pageStoreVer = 2;
        Set<String> indexes = null;
        String destFile = null;
        boolean checkParts = false;

        int indexReaderArgsCnt = 7;

        while (argIter.hasNextSubArg() && indexReaderArgsCnt-- > 0) {
            String nextArg = argIter.nextArg("");

            IndexReaderCommandArg arg = CommandArgUtils.of(nextArg, IndexReaderCommandArg.class);

            switch (arg) {
                case DIR:
                    dir = argIter.nextArg("Partition directory, where index.bin and (optionally) partition files are located");

                    break;

                case PART_CNT:
                    partCnt = argIter.nextIntArg("Full partitions count in cache group.");

                    break;

                case PAGE_SIZE:
                    pageSize = argIter.nextIntArg("Page size.");

                    break;

                case PAGE_STORE_VER:
                    pageStoreVer = argIter.nextIntArg("Page store version.");

                    break;

                case INDEXES:
                    indexes = argIter.nextStringSet("Index tree names. You can specify index tree names that will be processed, " +
                            "separated by comma without spaces, other index trees will be skipped. Default value: null. " +
                            "Index tree names are not the same as index names, they have format cacheId_typeId_indexName##H2Tree%segmentNumber, " +
                            "e.g. 2652_885397586_T0_F0_F1_IDX##H2Tree%0. You can see them in utility output, in traversal information " +
                            "sections (RECURSIVE and HORIZONTAL).");

                    break;

                case DEST_FILE:
                    destFile = argIter.nextArg("File to print the report to.");

                    break;

                case CHECK_PARTS:
                    checkParts = true;

                    break;
            }
        }

        args = new Arguments(dir, partCnt, pageSize, pageStoreVer, indexes, destFile, checkParts);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return INDEX_READER.text().toUpperCase();
    }
}
