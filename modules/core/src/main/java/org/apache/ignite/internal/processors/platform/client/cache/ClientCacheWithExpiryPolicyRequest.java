package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicy;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import javax.cache.expiry.ExpiryPolicy;

/**
 * Client cache with expiry policy request.
 */
public class ClientCacheWithExpiryPolicyRequest extends ClientCacheRequest {
    /** Cache configuration. */
    private final ExpiryPolicy expiryPolicy;

    /**
     * Constructor.
     *
     * @param reader Reader.
     * @param ver Client version.
     */
    public ClientCacheWithExpiryPolicyRequest(BinaryRawReader reader, ClientListenerProtocolVersion ver) {
        super(reader);

        expiryPolicy = new PlatformExpiryPolicy(reader.readLong(), reader.readLong(), reader.readLong());
    }

    /** {@inheritDoc} */
    @Override
    public ClientResponse process(ClientConnectionContext ctx) {
        IgniteCache cache = cache(ctx);
        IgniteCache igniteCache = cache.withExpiryPolicy(expiryPolicy);


        return super.process(ctx);
    }
}
