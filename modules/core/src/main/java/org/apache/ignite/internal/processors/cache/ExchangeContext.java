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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.IgniteFeatures.PME_FREE_SWITCH;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager.exchangeProtocolVersion;

/**
 *
 */
public class ExchangeContext {
    /** */
    public static final String IGNITE_EXCHANGE_COMPATIBILITY_VER_1 = "IGNITE_EXCHANGE_COMPATIBILITY_VER_1";

    /** Cache groups to request affinity for during local join exchange. */
    private Set<Integer> requestGrpsAffOnJoin;

    /** Per-group affinity fetch on join (old protocol). */
    private boolean fetchAffOnJoin;

    /** Wait-free affinity switch was performed for current exchange. */
    private boolean exchangeFreeSwitch;

    /** Merges allowed flag. */
    private final boolean merge;

    /** */
    private final ExchangeDiscoveryEvents evts;

    /** */
    private final boolean compatibilityNode = getBoolean(IGNITE_EXCHANGE_COMPATIBILITY_VER_1, false);

    /** */
    private final boolean bltForVolatileCachesSup = isFeatureEnabled(IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE);

    /** Free switch is supported for current exchange topology. */
    private final boolean supFreeSwitch;

    /**
     * @param crd Coordinator flag.
     * @param fut Exchange future.
     */
    public ExchangeContext(boolean crd, GridDhtPartitionsExchangeFuture fut) {
        int protocolVer = exchangeProtocolVersion(fut.firstEventCache().minimumNodeVersion());

        supFreeSwitch =
            allNodesSupports(fut.sharedContext().kernalContext(), fut.firstEventCache().allNodes(), PME_FREE_SWITCH);

        // Tests if free switch is supported for current exchange.
        if (!compatibilityNode &&
            fut.wasRebalanced() &&
            fut.isBaselineNodeFailed() &&
            supFreeSwitch &&
            (bltForVolatileCachesSup ||
                CU.dataRegions(fut.firstEventCache().allNodes(),
                    fut.sharedContext().kernalContext().marshallerContext().jdkMarshaller(),
                    U.resolveClassLoader(fut.sharedContext().kernalContext().config())).
                    allMatch(DataRegionConfiguration::isPersistenceEnabled))) {
            exchangeFreeSwitch = true;
            merge = false;
        }
        else if (compatibilityNode || (crd && fut.localJoinExchange())) {
            fetchAffOnJoin = true;
            merge = false;
        }
        else {
            boolean startCaches = fut.exchangeId().isJoined() &&
                fut.sharedContext().cache().hasCachesReceivedFromJoin(fut.exchangeId().eventNode());

            fetchAffOnJoin = protocolVer == 1;

            merge = !startCaches &&
                protocolVer > 1 &&
                fut.firstEvent().type() != EVT_DISCOVERY_CUSTOM_EVT;
        }

        evts = new ExchangeDiscoveryEvents(fut);
    }

    /**
     * @param node Node.
     * @return {@code True} if node supports exchange merge protocol.
     */
    boolean supportsMergeExchanges(ClusterNode node) {
        return !compatibilityNode && exchangeProtocolVersion(node.version()) > 1;
    }

    /**
     * @return {@code True} if exchange topology is compatible with free switch optimization.
     */
    public boolean supportsFreeSwitch() {
        return supFreeSwitch;
    }

    /**
     * @return Discovery events.
     */
    public ExchangeDiscoveryEvents events() {
        return evts;
    }

    /**
     * @return {@code True} if on local join need fetch affinity per-group (old protocol),
     *      otherwise affinity is sent in {@link GridDhtPartitionsFullMessage}.
     */
    public boolean fetchAffinityOnJoin() {
        return fetchAffOnJoin;
    }

    /**
     * @return {@code True} if it's safe to perform PME-free affinity switch.
     */
    public boolean exchangeFreeSwitch() {
        return exchangeFreeSwitch;
    }

    /**
     * @param grpId Cache group ID.
     */
    synchronized void addGroupAffinityRequestOnJoin(Integer grpId) {
        if (requestGrpsAffOnJoin == null)
            requestGrpsAffOnJoin = new HashSet<>();

        requestGrpsAffOnJoin.add(grpId);
    }

    /**
     * @return Groups to request affinity for.
     */
    @Nullable public synchronized Set<Integer> groupsAffinityRequestOnJoin() {
        return requestGrpsAffOnJoin;
    }

    /**
     * @return {@code True} if exchanges merge is allowed during current exchange.
     */
    public boolean mergeExchanges() {
        return merge;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExchangeContext.class, this);
    }
}
