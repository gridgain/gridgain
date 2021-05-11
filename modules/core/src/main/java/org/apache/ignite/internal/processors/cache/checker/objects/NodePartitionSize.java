package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntRWHashMap;

public class NodePartitionSize extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    public String cacheName;

    public boolean inProgress;

    public boolean isFinished;

    public KeyCacheObject lastKey;

//    /** Partition size. */
//    public long oldStorageSize;

    /** */
    public long oldCacheSize;
//    public IntMap<AtomicLong> oldCacheSizes = new IntRWHashMap();

//    /** Partition size. */
//    public long newStorageSize;

    /** */
    public long newCacheSize;
//    public IntMap<AtomicLong> newCacheSizes = new IntRWHashMap();

    public NodePartitionSize() {
    }

    public NodePartitionSize(String cacheName) {
        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(cacheName);
        out.writeBoolean(inProgress);
        out.writeBoolean(isFinished);
        out.writeObject(lastKey);
//        out.writeObject(oldStorageSize);
        out.writeLong(oldCacheSize);
//        out.writeObject(newStorageSize);
        out.writeLong(newCacheSize);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = (String)in.readObject();
        inProgress = in.readBoolean();
        isFinished = in.readBoolean();
        lastKey = (KeyCacheObject)in.readObject();

        oldCacheSize = in.readLong();
//        oldCacheSizes = (IntMap<AtomicLong>)in.readObject();
        newCacheSize = in.readLong();
//        newCacheSizes = (IntMap<AtomicLong>)in.readObject();
    }
}
