package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

public class NodePartitionSize extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    public boolean inProgress;

    public KeyCacheObject lastKey;

    public long oldSize;

    public long newSize;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(inProgress);
        out.writeObject(lastKey);
        out.writeLong(oldSize);
        out.writeLong(newSize);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        inProgress = in.readBoolean();
        lastKey = (KeyCacheObject)in.readObject();
        oldSize = in.readLong();
        newSize = in.readLong();
    }
}
