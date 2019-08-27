package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;

public class ClientClusterRequest extends ClientRequest {

    private final long pointer;

    public ClientClusterRequest(BinaryRawReader reader, long pointer) {
        super(reader);
        this.pointer = pointer;
    }
}
