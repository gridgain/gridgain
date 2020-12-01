package org.apache.ignite.internal.commandline.indexreader;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.mandatoryArg;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;
import static org.apache.ignite.internal.commandline.indexreader.PartReader.Args.*;
import static org.apache.ignite.internal.commandline.indexreader.PartReader.Args.DEST_FILE;
import static org.apache.ignite.internal.commandline.indexreader.PartReader.Args.PAGE_SIZE;
import static org.apache.ignite.internal.commandline.indexreader.PartReader.Args.PAGE_STORE_VER;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.T_PART_META;

public class PartReader extends IgniteIndexReader {
    private final FilePageStore partStore;
    private final File partPath;
    /** {@link FilePageStore} factory by page store version. */
    private final FileVersionCheckingFactory storeFactory;
    /** Metrics updater. */
    private final LongAdderMetric allocationTracker = new LongAdderMetric("n", "d");
    private final Integer pageSize;
    private final int partNumber;

    /** */
    public PartReader(
        @Nullable PrintStream outStream,
        int pageSize,
        File path,
        int filePageStoreVer
    ) throws IgniteCheckedException {
        super(outStream, pageSize);
        this.pageSize = pageSize;
        partPath = path;

        if (partPath.isDirectory() || !partPath.getName().contains("part-"))
            throw new IllegalArgumentException("Wrong file name argument(shouldn't be a directory and has spacial name format).");

        String partFileName = partPath.getName();
        partNumber = Integer.parseInt(partFileName.substring(partFileName.lastIndexOf("-") + 1, partFileName.indexOf(".bin")));

        storeFactory = new FileVersionCheckingFactory(
                new AsyncFileIOFactory(),
                new AsyncFileIOFactory(),
                new DataStorageConfiguration().setPageSize(pageSize)
        ) {
            /** {@inheritDoc} */
            @Override public int latestVersion() {
                return filePageStoreVer;
            }
        };

        partStore = (FilePageStore)storeFactory.createPageStore(FLAG_DATA, partPath, allocationTracker);
        if (nonNull(partStore))
            partStore.ensure();

    }

    public void read() {
        outStream.println("Check part=" + partNumber);

        try {
            Map<Short, Long> metaPages = findPages(partNumber, FLAG_DATA, partStore, singleton(T_PART_META));

            long partMetaId = metaPages.get(T_PART_META);

            long realSize = doWithBuffer((buf, addr) -> {
                readPage(partStore, partMetaId, buf);

                PagePartitionMetaIO partMetaIO = PageIO.getPageIO(addr);

                outStream.println("Meta page size=" + partMetaIO.getSize(addr));

                long cacheDataTreeRoot = partMetaIO.getTreeRoot(addr);

                TreeTraversalInfo cacheDataTreeInfo =
                        horizontalTreeScan(partStore, cacheDataTreeRoot, "dataTree-" + partNumber, new ItemsListStorage());

                return cacheDataTreeInfo.itemStorage.size();
            });

            outStream.println("Real size=" + realSize);

        } catch (IgniteCheckedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Entry point.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        AtomicReference<CLIArgumentParser> parserRef = new AtomicReference<>();

        List<CLIArgument> argsConfiguration = asList(
                mandatoryArg(
                        PART_PATH.arg(),
                        "partition directory, where " + INDEX_FILE_NAME + " and (optionally) partition files are located.",
                        String.class
                ),
                optionalArg(PAGE_SIZE.arg(), "page size.", Integer.class, () -> 4096),
                optionalArg(PAGE_STORE_VER.arg(), "page store version.", Integer.class, () -> 2),
                optionalArg(DEST_FILE.arg(),
                        "file to print the report to (by default report is printed to console).", String.class, () -> null)
        );

        CLIArgumentParser p = new CLIArgumentParser(argsConfiguration);

        parserRef.set(p);

        if (args.length == 0) {
            System.out.println(p.usage());

            return;
        }

        String partPath = p.get(PART_PATH.arg());
        int pageSize = p.get(PAGE_SIZE.arg());
        int pageStoreVer = p.get(PAGE_STORE_VER.arg());
        String destFile = p.get(DEST_FILE.arg());

        try (PartReader reader = new PartReader(
                isNull(destFile) ? null : new PrintStream(destFile),
                pageSize,
                new File(partPath),
                pageStoreVer
        )) {
            reader.read();
        }
    }

    /**
     * Enum of possible utility arguments.
     */
    public enum Args {
        /** */
        PART_PATH("--part-path"),
        /** */
        PAGE_SIZE("--page-size"),
        /** */
        PAGE_STORE_VER("--page-store-ver"),
        /** */
        DEST_FILE("--dest-file");

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
