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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.util.UUID;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRequiredFeatureSupport;

/** */
@TcpDiscoveryRequiredFeatureSupport(feature = IgniteFeatures.DISTRIBUTED_METASTORAGE)
class DistributedMetaStorageCasAckMessage extends DistributedMetaStorageUpdateAckMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final boolean updated;

    /** */
    public DistributedMetaStorageCasAckMessage(UUID reqId, String errorMsg, boolean updated) {
        super(reqId, errorMsg);

        this.updated = updated;
    }

    /** */
    public boolean updated() {
        return updated;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedMetaStorageCasAckMessage.class, this);
    }
}
