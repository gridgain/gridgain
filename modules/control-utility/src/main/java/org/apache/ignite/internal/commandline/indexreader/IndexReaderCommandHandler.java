package org.apache.ignite.internal.commandline.indexreader;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.commandline.Command.usageParams;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.errorMessage;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.indexreader.IndexReaderCommandArg.CHECK_PARTS;
import static org.apache.ignite.internal.commandline.indexreader.IndexReaderCommandArg.DEST_FILE;
import static org.apache.ignite.internal.commandline.indexreader.IndexReaderCommandArg.DIR;
import static org.apache.ignite.internal.commandline.indexreader.IndexReaderCommandArg.INDEXES;
import static org.apache.ignite.internal.commandline.indexreader.IndexReaderCommandArg.PAGE_SIZE;
import static org.apache.ignite.internal.commandline.indexreader.IndexReaderCommandArg.PAGE_STORE_VER;
import static org.apache.ignite.internal.commandline.indexreader.IndexReaderCommandArg.PART_CNT;

import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLoggerFileHandler;
import org.apache.ignite.logger.java.JavaLoggerFormatter;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.PrintStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

/**
 * Class that execute index reader commands passed via command line.
 */
public class IndexReaderCommandHandler {
    /** */
    static final String CMD_HELP = "--help";

    /** */
    public static final String DELIM = "--------------------------------------------------------------------------------";

    /** Utility name. */
    public static final String UTILITY_NAME = "index-reader.(sh|bat)";

    /** */
    public static final int EXIT_CODE_OK = 0;

    /** */
    public static final int EXIT_CODE_INVALID_ARGUMENTS = 1;

    /** */
    public static final int EXIT_CODE_UNEXPECTED_ERROR = 2;

    /** JULs logger. */
    protected final Logger logger;

    /** Session. */
    protected final String ses = U.id8(UUID.randomUUID());

    /** Date format. */
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    /**
     *
     */
    public IndexReaderCommandHandler() {
        logger = setupJavaLogger();
    }

    /**
     * @param logger Logger to use.
     */
    public IndexReaderCommandHandler(Logger logger) {
        this.logger = logger;
    }

    /**
     * @return prepared JULs logger.
     */
    private Logger setupJavaLogger() {
        Logger result = initLogger(CommandHandler.class.getName() + "Log");

        // Adding logging to file.
        try {
            String absPathPattern = new File(JavaLoggerFileHandler.logDirectory(U.defaultWorkDirectory()), "control-utility-%g.log").getAbsolutePath();

            FileHandler fileHandler = new FileHandler(absPathPattern, 5 * 1024 * 1024, 5);

            fileHandler.setFormatter(new JavaLoggerFormatter());

            result.addHandler(fileHandler);
        }
        catch (Exception e) {
            System.out.println("Failed to configure logging to file");
        }

        // Adding logging to console.
        result.addHandler(setupStreamHandler());

        return result;
    }

    /**
     * @return StreamHandler with empty formatting
     */
    public static StreamHandler setupStreamHandler() {
        return new StreamHandler(System.out, new Formatter() {
            @Override public String format(LogRecord record) {
                return record.getMessage() + "\n";
            }
        });
    }

    /**
     * Initialises JULs logger with basic settings
     * @param loggerName logger name. If {@code null} anonymous logger is returned.
     * @return logger
     */
    public static Logger initLogger(@Nullable String loggerName) {
        Logger result;

        if (loggerName == null)
            result = Logger.getAnonymousLogger();
        else
            result = Logger.getLogger(loggerName);

        result.setLevel(Level.INFO);
        result.setUseParentHandlers(false);

        return result;
    }

    public static void main(String[] args) {
        IndexReaderCommandHandler hnd = new IndexReaderCommandHandler();

        System.exit(hnd.execute(Arrays.asList(args)));
    }

    /**
     * Print commands usage.
     */
    protected void printCommandsUsage() {
        Arrays.stream(CommandList.values()).filter(this::skipCommand).forEach(c -> c.command().printUsage(logger));
    }

    /**
     * Returns {@code true}, if the given command should be skipped.
     * @param cmd Command.
     */
    private boolean skipCommand(CommandList cmd) {
        return true;
    }

    /**
     * Parse and execute command.
     *
     * @param rawArgs Arguments to parse and execute.
     * @return Exit code.
     */
    public int execute(List<String> rawArgs) {
        LocalDateTime startTime = LocalDateTime.now();

        Thread.currentThread().setName("session=" + ses);

        logger.info("Index reader utility [ver. " + ACK_VER_STR + "]");
        logger.info(COPYRIGHT);
        logger.info("User: " + System.getProperty("user.name"));
        logger.info("Time: " + startTime.format(formatter));

        Throwable err = null;

        try {
            if (F.isEmpty(rawArgs) || (rawArgs.size() == 1 && CMD_HELP.equalsIgnoreCase(rawArgs.get(0)))) {
                printHelp();

                return EXIT_CODE_OK;
            }

            final Arguments args = parseAndValidate(new CommandArgIterator(rawArgs.iterator(), Collections.emptySet()));

            final IgniteIndexReaderFilePageStoreFactoryImpl factory = new IgniteIndexReaderFilePageStoreFactoryImpl(
                    new File(args.dir()),
                    args.pageSize(),
                    args.partCnt(),
                    args.pageStoreVer()
            );

            try (IgniteIndexReader idxReader = new IgniteIndexReader(
                    args.indexes() != null ? idx -> args.indexes().contains(idx) : null,
                    args.checkParts(),
                    args.destFile() != null ? new PrintStream(args.destFile()) : null,
                    factory
            )) {
                idxReader.readIdx();
            }

            return EXIT_CODE_OK;
        }
        catch (IllegalArgumentException e) {
            logger.severe("Check arguments. " + errorMessage(e));

            return EXIT_CODE_INVALID_ARGUMENTS;
        }
        catch (Throwable e) {
            logger.severe(errorMessage(e));

            err = e;

            return EXIT_CODE_UNEXPECTED_ERROR;
        }
        finally {
            LocalDateTime endTime = LocalDateTime.now();

            Duration diff = Duration.between(startTime, endTime);

            if (nonNull(err))
                logger.info("Error stack trace:" + System.lineSeparator() + X.getFullStackTrace(err));

            logger.info("Index reader utility has completed execution at: " + endTime.format(formatter));
            logger.info("Execution time: " + diff.toMillis() + " ms");

            Arrays.stream(logger.getHandlers())
                    .filter(handler -> handler instanceof FileHandler)
                    .forEach(Handler::close);
        }
    }

    /**
     * Parse command line arguments.
     * @return Command parsed arguments.
     * @throws IllegalArgumentException In case arguments aren't valid.
     */
    private Arguments parseAndValidate(CommandArgIterator argIter) {
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

            if (isNull(arg) && !nextArg.isEmpty())
                throw new IllegalArgumentException("Unexpected argument: " + nextArg);

            switch (arg) {
                case DIR:
                    dir = argIter.nextArg("partition directory, where index.bin and (optionally) partition files are located");

                    break;

                case PART_CNT:
                    partCnt = argIter.nextIntArg("full partitions count in cache group.");

                    break;

                case PAGE_SIZE:
                    pageSize = argIter.nextIntArg("page size.");

                    break;

                case PAGE_STORE_VER:
                    pageStoreVer = argIter.nextIntArg("page store version.");

                    break;

                case INDEXES:
                    indexes = argIter.nextStringSet("index tree names.");

                    break;

                case DEST_FILE:
                    destFile = argIter.nextArg("Expected file to print the report to.");

                    break;

                case CHECK_PARTS:
                    checkParts = true;

                    break;
            }
        }

        return new Arguments(dir, partCnt, pageSize, pageStoreVer, indexes, destFile, checkParts);
    }

    /**
     * @return Utility name.
     */
    protected String utilityName() {
        return UTILITY_NAME;
    }

    /** */
    private void printHelp() {
        logger.info("The utility can analyze index.bin and optionally partitions. " +
                "The command has the following syntax:");
        logger.info("");

        logger.info(DOUBLE_INDENT + CommandLogger.join(" ", UTILITY_NAME, CommandLogger.join(" ",
                DIR, optional(PART_CNT), optional(PAGE_SIZE), optional(PAGE_STORE_VER), optional(INDEXES), optional(DEST_FILE), optional(CHECK_PARTS))));

        logger.info("");
        logger.info(DOUBLE_INDENT + "Parameters:");

        usageParams(Arrays.stream(IndexReaderCommandArg.values())
                .collect(toMap(IndexReaderCommandArg::argName, IndexReaderCommandArg::desc)), DOUBLE_INDENT + INDENT, logger);

        logger.info("");

        logger.info("Exit codes:");
        logger.info(DOUBLE_INDENT + EXIT_CODE_OK + " - successful execution.");
        logger.info(DOUBLE_INDENT + EXIT_CODE_INVALID_ARGUMENTS + " - invalid arguments.");
        logger.info(DOUBLE_INDENT + EXIT_CODE_UNEXPECTED_ERROR + " - unexpected error.");
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
            return S.toString(Arguments.class, this);
        }
    }
}
