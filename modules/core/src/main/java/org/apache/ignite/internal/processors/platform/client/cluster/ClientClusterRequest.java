package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;

/**
 * Client cluster request.
 */
public abstract class ClientClusterRequest extends ClientRequest {

    /** Cluster object pointer. */
    protected final long clusterId;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientClusterRequest(BinaryRawReader reader) {
        super(reader);

        clusterId = reader.readLong();
    }
}
