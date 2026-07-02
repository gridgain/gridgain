/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.client.ClientCluster;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;

import java.util.concurrent.CompletionException;

/**
 * Implementation of {@link ClientCluster}.
 */
class ClientClusterImpl extends ClientClusterGroupImpl implements ClientCluster {
    /** Default cluster group. */
    private final ClientClusterGroupImpl dfltClusterGrp;

    /**
     * Constructor.
     */
    ClientClusterImpl(ReliableChannel ch, ClientBinaryMarshaller marsh) {
        super(ch, marsh);

        dfltClusterGrp = (ClientClusterGroupImpl)forServers();
    }

    /** {@inheritDoc} */
    @Override public ClusterState state() {
        return ClientUtils.syncResult(stateAsync());
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<ClusterState> stateAsync() {
        return new IgniteClientFutureImpl<>(
                ch.serviceAsync(
                        ClientOperation.CLUSTER_GET_STATE,
                        req -> checkClusterApiSupported(req.clientChannel().protocolCtx()),
                        res -> ClusterState.fromOrdinal(res.in().readByte())
                ).handle(ClientClusterImpl::errorHandler)
        );
    }

    /** {@inheritDoc} */
    @Override public void state(ClusterState newState) throws ClientException {
        ClientUtils.syncResult(stateAsync(newState));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> stateAsync(ClusterState newState) throws ClientException {
        return new IgniteClientFutureImpl<>(
                ch.serviceAsync(
                        ClientOperation.CLUSTER_CHANGE_STATE,
                        req -> {
                            ProtocolContext protocolCtx = req.clientChannel().protocolCtx();

                            checkClusterApiSupported(protocolCtx);

                            if (newState.ordinal() > 1 && !protocolCtx.isFeatureSupported(ProtocolBitmaskFeature.CLUSTER_API)) {
                                throw new ClientFeatureNotSupportedByServerException("State " + newState.name() + " is not " +
                                        "supported by the server");
                            }

                            req.out().writeByte((byte)newState.ordinal());
                        },
                        null
                ).handle((v, err) -> errorHandler((Void) v, err))
        );
    }

    /** {@inheritDoc} */
    @Override public boolean disableWal(String cacheName) throws ClientException {
        return ClientUtils.syncResult(disableWalAsync(cacheName));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> disableWalAsync(String cacheName) throws ClientException {
        return changeWalStateAsync(cacheName, false);
    }

    /** {@inheritDoc} */
    @Override public boolean enableWal(String cacheName) throws ClientException {
        return ClientUtils.syncResult(enableWalAsync(cacheName));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> enableWalAsync(String cacheName) throws ClientException {
        return changeWalStateAsync(cacheName, true);
    }

    /** {@inheritDoc} */
    @Override public boolean isWalEnabled(String cacheName) {
        return ClientUtils.syncResult(isWalEnabledAsync(cacheName));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> isWalEnabledAsync(String cacheName) {
        return new IgniteClientFutureImpl<>(
                ch.serviceAsync(
                        ClientOperation.CLUSTER_GET_WAL_STATE,
                        req -> {
                            checkClusterApiSupported(req.clientChannel().protocolCtx());

                            try (BinaryRawWriterEx writer = utils.createBinaryWriter(req.out())) {
                                writer.writeString(cacheName);
                            }
                        },
                        res -> res.in().readBoolean()
                ).handle(ClientClusterImpl::errorHandler)
        );
    }

    /**
     * @param cacheName Cache name.
     * @param enable {@code True} if WAL should be enabled, {@code false} if WAL should be disabled.
     */
    private IgniteClientFuture<Boolean> changeWalStateAsync(String cacheName, boolean enable) throws ClientException {
        return new IgniteClientFutureImpl<>(
                ch.serviceAsync(ClientOperation.CLUSTER_CHANGE_WAL_STATE,
                        req -> {
                            checkClusterApiSupported(req.clientChannel().protocolCtx());

                            try (BinaryRawWriterEx writer = utils.createBinaryWriter(req.out())) {
                                writer.writeString(cacheName);
                                writer.writeBoolean(enable);
                            }
                        },
                        res -> res.in().readBoolean()
                ).handle(ClientClusterImpl::errorHandler)
        );
    }

    /**
     * Check that Cluster API is supported by server.
     *
     * @param protocolCtx Protocol context.
     */
    private void checkClusterApiSupported(ProtocolContext protocolCtx)
        throws ClientFeatureNotSupportedByServerException {
        if (!protocolCtx.isFeatureSupported(ProtocolVersionFeature.CLUSTER_API) &&
            !protocolCtx.isFeatureSupported(ProtocolBitmaskFeature.CLUSTER_API))
            throw new ClientFeatureNotSupportedByServerException(ProtocolBitmaskFeature.CLUSTER_API);
    }

    /**
     * Default cluster group ("for servers").
     */
    ClientClusterGroupImpl defaultClusterGroup() {
        return dfltClusterGrp;
    }

    private static <T> T errorHandler(T v, Throwable err) {
        if (err != null) {
            Throwable cause = (err instanceof CompletionException) ? err.getCause() : err;
            if (cause instanceof ClientError) {
                throw new ClientException(cause);
            }

            throw (RuntimeException) err;
        }

        return v;
    }
}
