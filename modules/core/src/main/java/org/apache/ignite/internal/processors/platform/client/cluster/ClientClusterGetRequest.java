package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.platform.client.*;
import org.apache.ignite.internal.processors.platform.cluster.PlatformClusterGroup;

/**
 * Cluster get request.
 */
public class ClientClusterGetRequest extends ClientRequest {
    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientClusterGetRequest(BinaryRawReader reader) {
        super(reader);
    }

    /** {@inheritDoc} */
    @Override
    public ClientResponse process(ClientConnectionContext ctx) {

        IgniteClusterEx cluster = ctx.kernalContext().grid().cluster();
        ClientCluster clientCluster = new ClientCluster(cluster);

        long resId = ctx.resources().put(clientCluster);
        return new ClientLongResponse(requestId(), resId);
    }
}
