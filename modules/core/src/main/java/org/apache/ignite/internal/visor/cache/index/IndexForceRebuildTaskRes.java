package org.apache.ignite.internal.visor.cache.index;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result of {@link IndexForceRebuildTask}.
 */
public class IndexForceRebuildTaskRes extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Caches for which indexes rebuild was triggered. */
    private Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild;

    /** Caches with indexes rebuild in progress. */
    private Set<IndexRebuildStatusInfoContainer> cachesWithRebuildInProgress;

    /** Names of caches that were not found. */
    private Set<String> notFoundCacheNames;

    /**
     * Empty constructor required for Serializable.
     */
    public IndexForceRebuildTaskRes() {
        // No-op.
    }

    /** */
    public IndexForceRebuildTaskRes(
        Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild,
        Set<IndexRebuildStatusInfoContainer> cachesWithRebuildInProgress,
        Set<String> notFoundCacheNames)
    {
        this.cachesWithStartedRebuild = cachesWithStartedRebuild;
        this.cachesWithRebuildInProgress = cachesWithRebuildInProgress;
        this.notFoundCacheNames = notFoundCacheNames;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, cachesWithStartedRebuild);
        U.writeCollection(out, cachesWithRebuildInProgress);
        U.writeCollection(out, notFoundCacheNames);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cachesWithStartedRebuild    = U.readSet(in);
        cachesWithRebuildInProgress = U.readSet(in);
        notFoundCacheNames          = U.readSet(in);
    }

    /** */
    public Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild() {
        return cachesWithStartedRebuild;
    }

    /** */
    public Set<IndexRebuildStatusInfoContainer> cachesWithRebuildInProgress() {
        return cachesWithRebuildInProgress;
    }

    /** */
    public Set<String> notFoundCacheNames() {
        return notFoundCacheNames;
    }
}
