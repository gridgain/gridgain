package org.apache.ignite.internal.commandline.cache.argument;

import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;

/**
 * {@link CacheSubcommands#RESET_LOST_PARTITIONS} command arguments.
 * */
public enum ResetLostPartitionsCommandArg implements CommandArg {

    /** All caches. */
    ALL_CACHES_MODE("--all");

    /** Option name. */
    private final String name;

    /** */
    ResetLostPartitionsCommandArg(String name) {
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
