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

package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.discovery.IncompleteDeserializationException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.service.ServiceDeploymentActions;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache change batch.
 */
public class DynamicCacheChangeBatch implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Discovery custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** Request ID -> Initiator Node ID mapping extracted from {@link DynamicCacheChangeBatch#reqs} collection
     * for a situation with incomplete deserialization when no requests can be obtained from the collection. */
    private Map<UUID, UUID> cacheReqsMapping;

    /** Cache name -> Deployment ID mapping */
    private Map<String, IgniteUuid> cacheTemplateReqsMapping;

    /** Change requests. */
    @GridToStringInclude
    private Collection<DynamicCacheChangeRequest> reqs;

    /** Cache updates to be executed on exchange. */
    private transient ExchangeActions exchangeActions;

    /** */
    private boolean startCaches;

    /** Restarting caches. */
    private Set<String> restartingCaches;

    /** Affinity (cache related) services updates to be processed on services deployment process. */
    @GridToStringExclude
    @Nullable private transient ServiceDeploymentActions serviceDeploymentActions;

    /** */
    private ClassNotFoundException deserEx;

    /**
     * @param reqs Requests.
     */
    public DynamicCacheChangeBatch(Collection<DynamicCacheChangeRequest> reqs) {
        assert !F.isEmpty(reqs) : reqs;

        cacheReqsMapping = reqs.stream()
            .collect(
                Collectors.toMap(
                    DynamicCacheChangeRequest::requestId,
                    DynamicCacheChangeRequest::initiatingNodeId
            )
        );

        cacheTemplateReqsMapping = reqs.stream()
            .filter(req -> req.deploymentId() != null)
            .collect(
                Collectors.toMap(
                    DynamicCacheChangeRequest::cacheName,
                    DynamicCacheChangeRequest::deploymentId
            )
        );

        this.reqs = reqs;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return mgr.createDiscoCacheOnCacheChange(topVer, discoCache);
    }

    /**
     * @return Collection of change requests.
     */
    public Collection<DynamicCacheChangeRequest> requests() {
        return reqs;
    }

    /**
     * @return {@code True} if request should trigger partition exchange.
     */
    public boolean exchangeNeeded() {
        return exchangeActions != null;
    }

    /**
     * @return Cache updates to be executed on exchange.
     */
    public ExchangeActions exchangeActions() {
        return exchangeActions;
    }

    /**
     * @param exchangeActions Cache updates to be executed on exchange.
     */
    void exchangeActions(ExchangeActions exchangeActions) {
        assert exchangeActions != null && !exchangeActions.empty() : exchangeActions;

        this.exchangeActions = exchangeActions;
    }

    /**
     * @return Services deployment actions to be processed on services deployment process.
     */
    @Nullable public ServiceDeploymentActions servicesDeploymentActions() {
        return serviceDeploymentActions;
    }

    /**
     * @param serviceDeploymentActions Services deployment actions to be processed on services deployment process.
     */
    public void servicesDeploymentActions(ServiceDeploymentActions serviceDeploymentActions) {
        this.serviceDeploymentActions = serviceDeploymentActions;
    }

    /**
     * @return {@code True} if required to start all caches on client node.
     */
    public boolean startCaches() {
        return startCaches;
    }

    /**
     * @param restartingCaches Restarting caches.
     */
    public DynamicCacheChangeBatch restartingCaches(Set<String> restartingCaches) {
        this.restartingCaches = restartingCaches;

        return this;
    }

    /**
     * @return Set of restarting caches.
     */
    public Set<String> restartingCaches() {
        return restartingCaches;
    }

    /**
     * @param startCaches {@code True} if required to start all caches on client node.
     */
    public void startCaches(boolean startCaches) {
        this.startCaches = startCaches;
    }

    /** */
    private void readObject(ObjectInputStream in) throws IOException {
        try {
            in.defaultReadObject();
        }
        catch (ClassNotFoundException cnfe) {
            deserEx = cnfe;

            throw new IncompleteDeserializationException(this);
        }
    }

    /**
     * Getter for an exception caught during message deserialization.
     *
     * @return Exception or {@code null} if deserialization was successful.
     */
    @Nullable public ClassNotFoundException deserializationException() {
        return deserEx;
    }

    /** */
    @Nullable public Map<UUID, UUID> cacheReqsMapping() {
        return cacheReqsMapping;
    }

    /** */
    @Nullable public Map<String, IgniteUuid> cacheTemplateReqsMapping() {
        return cacheTemplateReqsMapping;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheChangeBatch.class, this);
    }
}