package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientBooleanResponse;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cluster status request
 */
public class ClientClusterIsActiveRequest extends ClientClusterRequest {

    /**
     * Constructor.
     *
     * @param reader Reader/
     */
    public ClientClusterIsActiveRequest(BinaryRawReader reader) {
        super(reader);
    }

    /** {@inheritDoc} */
    @Override
    public ClientResponse process(ClientConnectionContext ctx) {
        ClientCluster clientCluster = ctx.resources().get(clusterId);
        return new ClientBooleanResponse(requestId(), clientCluster.isActive());
    }
}
