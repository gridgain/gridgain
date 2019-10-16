package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils;

import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;

/**
 * Client cache with expiry policy request.
 */
public class ClientCacheWithExpiryPolicyRequest extends ClientCacheRequest {
    /** Cache configuration. */
    private final Factory<? extends ExpiryPolicy> expiryPolicyFactoy;

    /**
     * Constructor.
     *
     * @param reader Reader.
     * @param ver Client version.
     */
    public ClientCacheWithExpiryPolicyRequest(BinaryRawReader reader, ClientListenerProtocolVersion ver) {
        super(reader);

        expiryPolicyFactoy = PlatformConfigurationUtils.readExpiryPolicyFactory(reader);
    }

    /** {@inheritDoc} */
    @Override
    public ClientResponse process(ClientConnectionContext ctx) {
        cache(ctx).withExpiryPolicy(expiryPolicyFactoy.create());
        return super.process(ctx);
    }
}
