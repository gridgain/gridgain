package org.apache.ignite.internal.commandline.cache.argument;

import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.AffinityViewCommand;

/**
 * Argument for {@link AffinityViewCommand}
 */
public enum AffinityViewCommandArg implements CommandArg {
    /** Current. */
    CURRENT("--current"),

    /** Ideal. */
    IDEAL("--ideal"),

    /** Diff. */
    DIFF("--diff"),

    /** Group name. */
    GROUP_NAME("--group_name");

    /** Option name. */
    private final String name;

    /**
     * @param name arg name
     */
    AffinityViewCommandArg(String name) {
        this.name = name;
    }

    /**
     * @return {@code name} without "--"
     */
    public String trimmedArgName() {
        if (name.startsWith("--"))
            return name.substring(2);
        else
            return name;
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
