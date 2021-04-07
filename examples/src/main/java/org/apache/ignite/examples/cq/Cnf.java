package org.apache.ignite.examples.cq;

import org.apache.ignite.configuration.IgniteConfiguration;

public class Cnf {
    public static IgniteConfiguration getCfg(String name, boolean isClient) {
        return
                new IgniteConfiguration().
                        setIgniteInstanceName(name).
                        setConsistentId(name).
                        setClientMode(isClient);
    }
}
