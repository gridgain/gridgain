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

import java.util.Collection;
import java.util.Set;

import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
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

    /**
     * @param reqs Requests.
     */
    public DynamicCacheChangeBatch(Collection<DynamicCacheChangeRequest> reqs) {
        assert !F.isEmpty(reqs) : reqs;

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
        return false;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheChangeBatch.class, this);
    }
}