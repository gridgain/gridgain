package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DR incremental transfer task args.
 */
public class VisorDrIncrementalTransferCmdArgs extends IgniteDataTransferObject {
    /** Serial version id. */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Snapshot id. */
    private long snapshotId;

    /** Data center id. */
    private byte dcId;

    /**
     * Default constructor.
     */
    public VisorDrIncrementalTransferCmdArgs() {
        // No-op.
    }

    /** */
    public VisorDrIncrementalTransferCmdArgs(String cacheName, long snapshotId, byte dcId) {
        this.cacheName = cacheName;
        this.snapshotId = snapshotId;
        this.dcId = dcId;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeUTF(cacheName);
        out.writeLong(snapshotId);
        out.writeByte(dcId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException {
        cacheName = in.readUTF();
        snapshotId = in.readLong();
        dcId = in.readByte();
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Snapshot id.
     */
    public long snapshotId() {
        return snapshotId;
    }

    /**
     * @return Data center id.
     */
    public byte dcId() {
        return dcId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrIncrementalTransferCmdArgs.class, this);
    }
}
