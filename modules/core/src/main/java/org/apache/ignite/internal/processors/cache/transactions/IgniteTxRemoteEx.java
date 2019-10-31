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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Local transaction API.
 */
public interface IgniteTxRemoteEx extends IgniteInternalTx {
    /**
     * @throws IgniteCheckedException If failed.
     */
    public void commitRemoteTx() throws IgniteCheckedException;

    /**
     *
     */
    public void rollbackRemoteTx();

    /**
     * @param baseVer Base version.
     * @param committedVers Committed version.
     * @param rolledbackVers Rolled back version.
     * @param pendingVers Pending versions.
     *
     * @throws GridDhtInvalidPartitionException If partition was invalidated.
     */
    public void doneRemote(GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pendingVers) throws GridDhtInvalidPartitionException;

    /**
     * @param cntrs Partition update indexes.
     */
    public void setPartitionUpdateCounters(long[] cntrs);
}
