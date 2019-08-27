package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

public class ClientClusterIsActiveRequest extends ClientClusterRequest {
    public ClientClusterIsActiveRequest(BinaryRawReader reader, long pointer) {
        super(reader, pointer);
    }

    @Override
    public ClientResponse process(ClientConnectionContext ctx) {
        return super.process(ctx);
    }
}
