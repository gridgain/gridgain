package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

public class ClientClusterGetRequest extends ClientRequest {
    public ClientClusterGetRequest(BinaryRawReader reader) {
        super(reader);
    }

    @Override
    public ClientResponse process(ClientConnectionContext ctx) {
        return super.process(ctx);
    }
}
