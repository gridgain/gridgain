package org.apache.ignite.internal.commandline.indexreader;

import org.apache.ignite.internal.commandline.argument.CommandArg;


public enum IndexReaderCommandArg implements CommandArg {
    /** Partition directory. */
    DIR("--dir"),

    /** Full partitions count in cache group. */
    PART_CNT("--part-cnt"),

    /** Page size. */
    PAGE_SIZE("--page-size"),

    /** Page store version. */
    PAGE_STORE_VER("--page-store-ver"),

    /** Index tree names. */
    INDEXES("--indexes"),

    /** File to print the report to. */
    DEST_FILE("--dest-file"),

    /** Check cache data tree in partition files and it's consistency with indexes. */
    CHECK_PARTS("--check-parts");


    /** Option name. */
    private final String name;

    /** */
    IndexReaderCommandArg(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
