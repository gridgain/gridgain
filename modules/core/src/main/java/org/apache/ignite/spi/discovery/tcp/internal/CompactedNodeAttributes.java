package org.apache.ignite.spi.discovery.tcp.internal;

public class CompactedNodeAttributes {
    final long lowTopVersion;
    final long highTopVersion;
    final Object[] attributes;


    public CompactedNodeAttributes(long lowTopVersion, long highTopVersion) {
        this.lowTopVersion = lowTopVersion;
        this.highTopVersion = highTopVersion;

        attributes = new Object[(int) (highTopVersion - lowTopVersion + 1)];

    }

//    public void addEntry(long topVer, Map)


}
