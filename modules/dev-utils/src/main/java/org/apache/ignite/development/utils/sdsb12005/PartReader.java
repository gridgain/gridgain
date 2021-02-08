package org.apache.ignite.development.utils.sdsb12005;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.development.utils.arguments.CLIArgument;
import org.apache.ignite.development.utils.arguments.CLIArgumentParser;
import org.apache.ignite.development.utils.indexreader.IgniteIndexReader;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.*;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;

public class PartReader extends IgniteIndexReader {
    private final FilePageStore partStore;
    private final File partPath;
    private final long metaPageId;
    /** {@link FilePageStore} factory by page store version. */
    private final FileVersionCheckingFactory storeFactory;
    /** Metrics updater. */
    private final LongAdderMetric allocationTracker = new LongAdderMetric("n", "d");
    private final int partNumber;

    /** */
    public PartReader(
        @Nullable PrintStream outStream,
        int pageSize,
        File pathDir,
        int filePageStoreVer,
        long metaPageId
    ) throws IgniteCheckedException {
        super(pageSize, outStream);
        this.metaPageId = metaPageId;

        if (!pathDir.isDirectory())
            throw new IllegalArgumentException("Wrong directory name argument.");

        partNumber = 0; //PageIdUtils.partId(metaPageId);
        String partFileName = String.format(FilePageStoreManager.PART_FILE_TEMPLATE, partNumber);

        partPath = new File(pathDir, partFileName);

        if (!partPath.exists()) {
            throw new IllegalArgumentException("Specified directory="
                + pathDir
                + " does not contain partition file="
                + partFileName
                + ", which stores metaPage=" + U.hexLong(metaPageId));
        }

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
            Map<Class, Long> metaPages = findPages(partNumber, FLAG_DATA, partStore, singleton(PagePartitionMetaIOV2.class));

            if(metaPages == null || metaPages.isEmpty()){

                outStream.println("Meta pages is empty! Return.");
                return;
            }

            long partMetaId = metaPages.get(PagePartitionMetaIOV2.class);

            doWithBuffer((buf, addr) -> {
                readPage(partStore, partMetaId, buf);

                PagePartitionMetaIOV2 partMetaIO = PageIO.getPageIO(addr);

                outStream.println(partMetaIO.printPage(addr, pageSize));

                long partMetaStoreReuseListRoot = partMetaIO.getPartitionMetaStoreReuseListRoot(addr);

                outStream.println("partMetaStoreReuseListRoot = " + partMetaStoreReuseListRoot + "partPath=" + partPath);
                printGapsLink(partMetaIO.getGapsLink(addr));

                printPagesListsInfo(getPageListsInfo(partMetaStoreReuseListRoot, partStore));

                return null;
            });

        } catch (IgniteCheckedException e) {
            e.printStackTrace();
        }

    }

    /** */
    private void printGapsLink(long gapsLink) throws IgniteCheckedException {
        SB sb = new SB();

        sb.a("Gaps Link content:").a("\n");

        long pageId = PageIdUtils.pageId(gapsLink);
        int itemId = PageIdUtils.itemId(gapsLink);

        sb.a("pageId=").a(pageId).a(",\n");
        sb.a("itemId=").a(itemId).a(",\n");

        ByteBuffer cntrUpdDataBuf = allocateBuffer(pageSize);

        long cntrUpdDataAddr = bufferAddress(cntrUpdDataBuf);

        readPage(partStore, pageId, cntrUpdDataBuf);

        AbstractDataPageIO dataPageIO = PageIO.getPageIO(cntrUpdDataAddr);

        sb.a("freeSpace=").a(dataPageIO.getFreeSpace(cntrUpdDataAddr)).a(",\n");

        sb.a("page = [\n\t").a(PageIO.printPage(cntrUpdDataAddr, pageSize)).a("],\n");

        sb.a("binPage=").a(U.toHexString(cntrUpdDataAddr, pageSize)).a("\n");

        outStream.println(sb.toString());
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
            CLIArgument.mandatoryArg(
                Args.PART_PATH.arg(),
                "partition path, where " + INDEX_FILE_NAME + " and partition files are located.",
                String.class
            ),
            CLIArgument.optionalArg(Args.PAGE_SIZE.arg(), "page size.", Integer.class, () -> 4096),
            CLIArgument.optionalArg(Args.PAGE_STORE_VER.arg(), "page store version.", Integer.class, () -> 2),
            CLIArgument.optionalArg(Args.DEST_FILE.arg(),
                "file to print the report to (by default report is printed to console).", String.class, () -> null)
        );

        CLIArgumentParser p = new CLIArgumentParser(argsConfiguration);

        parserRef.set(p);

        if (args.length == 0) {
            System.out.println(p.usage());

            return;
        }

        p.parse(asList(args).iterator());

//        String metaPageHex = p.get(Args.META_PAGE.arg());
//        long metaPage;
//        try {
//            metaPage = Long.parseLong(metaPageHex);
//        }
//        catch (NumberFormatException e) {
//            throw new IgniteCheckedException("Meta page string does not contain parsable Long:  " + metaPageHex);
//        }

        String partPath = p.get(Args.PART_PATH.arg());
        int pageSize = p.get(Args.PAGE_SIZE.arg());
        int pageStoreVer = p.get(Args.PAGE_STORE_VER.arg());
        String destFile = p.get(Args.DEST_FILE.arg());

        long metaPageID = 0L;

        try (PartReader reader = new PartReader(
            isNull(destFile) ? null : new PrintStream(destFile),
            pageSize,
            new File(partPath),
            pageStoreVer,
            metaPageID
        )) {
            reader.read();
        }
    }

    /**
     * Enum of possible utility arguments.
     */
    public enum Args {
        /** */
        PART_PATH("--partPath"),
        /** */
        PAGE_SIZE("--pageSize"),
        /** */
        PAGE_STORE_VER("--pageStoreVer"),
        /** */
        DEST_FILE("--destFile");

        /** */
        private final String arg;

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
