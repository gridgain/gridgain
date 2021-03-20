package org.apache.ignite.internal.commandline.indexreader;

import org.apache.ignite.internal.commandline.argument.CommandArg;

/** Index reader command arguments. */
public enum IndexReaderCommandArg implements CommandArg {
    /** Partition directory. */
    DIR("--dir", "partition directory, where index.bin and (optionally) partition files are located."),

    /** Full partitions count in cache group. */
    PART_CNT("--part-cnt", "full partitions count in cache group. Default value: 0."),

    /** Page size. */
    PAGE_SIZE("--page-size", "page size. Default value: 4096."),

    /** Page store version. */
    PAGE_STORE_VER("--page-store-ver", "page store version. Default value: 2."),

    /** Index tree names. */
    INDEXES("--indexes", "you can specify index tree names that will be processed, separated by comma without spaces, " +
            "other index trees will be skipped. Index tree names are not the same as index names, they have format " +
            "cacheId_typeId_indexName##H2Tree%segmentNumber, e.g. 2652_885397586_T0_F0_F1_IDX##H2Tree%0. You can see them in " +
            "utility output, in traversal information sections (RECURSIVE and HORIZONTAL)."),

    /** File to print the report to. */
    DEST_FILE("--dest-file", "file to print the report to (by default report is printed to console)."),

    /** Check cache data tree in partition files and it's consistency with indexes. */
    CHECK_PARTS("--check-parts", "check cache data tree in partition files and it's consistency with indexes. Default value: false.");

    /** Option name. */
    private final String name;

    /** Option description. */
    private final String desc;

    /** */
    IndexReaderCommandArg(String name, String desc) {
        this.name = name;
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    public String desc() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
