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

import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteHistoricalIterator;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;

/**
 * Iterator over supplied data for rebalancing.
 */
public interface IgniteRebalanceIterator extends GridCloseableIterator<CacheDataRow> {
    /**
     * @return {@code True} if this iterator is a historical iterator starting from the requested partition counter.
     */
    public boolean historical(int partId);

    /**
     * @param partId Partition ID.
     * @return {@code True} if all data for given partition was already returned.
     */
    public boolean isPartitionDone(int partId);

    /**
     * @param partId Partition ID.
     * @return {@code True} if partition was marked as missing.
     */
    public boolean isPartitionMissing(int partId);

    /**
     * Marks partition as missing.
     *
     * @param partId Partition ID.
     */
    public void setPartitionMissing(int partId);

    /**
     * Add iterator by partiotn specified.
     * @param partId Partition ID.
     * @param partIterator Partition iterartor.
     */
    public void addPartIterator(int partId, GridCloseableIterator<CacheDataRow> partIterator);

    /**
     * Close iterator for specific partition.
     * @param partId Prtition ID.
     *
     * @throws IgniteCheckedException In case of error.
     */
    public void closeForPart(int partId) throws IgniteCheckedException;

    /**
     * Partitions which will be full iterated.
     * @return Set of partiotns.
     */
    public Set<Integer> fullParts();

    /**
     * Replace historical data iterate.
     *
     * @throws IgniteCheckedException In case of error.
     */
    public void replaceHistorical(IgniteHistoricalIterator historicalIterator) throws IgniteCheckedException;

    /**
     * Return next element without moving iterator cursor to the next one.
     * @return Next element or {@code Null} if there is no more elements.
     */
    public CacheDataRow peek();
}
