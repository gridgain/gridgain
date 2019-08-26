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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.jetbrains.annotations.Nullable;

/**
 * Default iterator for rebalancing.
 */
public class IgniteRebalanceIteratorImpl implements IgniteRebalanceIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /** Iterators for full preloading, ordered by partition ID. */
    @Nullable private final LinkedHashMap<Integer, GridCloseableIterator<CacheDataRow>> fullIterators;

    /** Iterator for historical preloading. */
    @Nullable private IgniteHistoricalIterator historicalIterator;

    /** Partitions marked as missing. */
    private final Set<Integer> missingParts = new HashSet<>();

    /** Current full iterator. */
    private Map.Entry<Integer, GridCloseableIterator<CacheDataRow>> current;

    /** Next value. */
    private CacheDataRow cached;

    /** */
    private boolean reachedEnd;

    /** */
    private boolean closed;

    /** Set of loaded partiotns. */
    private HashSet<Integer> doneParts;

    /**
     *
     * @param fullIterators
     * @param historicalIterator
     * @throws IgniteCheckedException
     */
    public IgniteRebalanceIteratorImpl(
        LinkedHashMap<Integer, GridCloseableIterator<CacheDataRow>> fullIterators,
        IgniteHistoricalIterator historicalIterator) throws IgniteCheckedException {
        this.fullIterators = fullIterators;
        this.historicalIterator = historicalIterator;
        this.doneParts = new HashSet<>();

        advance(false);
    }

    /** */
    private synchronized void advance(boolean nextPart) throws IgniteCheckedException {
        if (fullIterators.isEmpty())
            reachedEnd = true;

        while (!reachedEnd && (current == null || !current.getValue().hasNextX()
            || missingParts.contains(current.getKey()) || nextPart)) {
            nextPart = false;

            if (current == null)
                current = fullIterators.entrySet().iterator().next();
            else {
                doneParts.add(current.getKey());

                Iterator<Map.Entry<Integer, GridCloseableIterator<CacheDataRow>>> iterator = fullIterators.entrySet().iterator();

                while (iterator.hasNext()) {
                    Map.Entry<Integer, GridCloseableIterator<CacheDataRow>> entry = iterator.next();

                    if (current == entry && iterator.hasNext()) {
                        current = iterator.next();

                        break;
                    }
                    else if (!iterator.hasNext()) {
                        assert current == entry;

                        current = null;

                        break;
                    }
                }

                if (current == null)
                    reachedEnd = true;
            }
        }

        assert current != null || reachedEnd;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean historical(int partId) {
        return historicalIterator != null && historicalIterator.contains(partId);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean isPartitionDone(int partId) {
        if (missingParts.contains(partId))
            return false;

        if (historical(partId))
            return historicalIterator.isDone(partId);

        return doneParts.contains(partId);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean isPartitionMissing(int partId) {
        return missingParts.contains(partId);
    }

    /** {@inheritDoc} */
    @Override public synchronized void setPartitionMissing(int partId) {
        missingParts.add(partId);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasNextX() throws IgniteCheckedException {
        if (historicalIterator != null && historicalIterator.hasNextX())
            return true;

        return current != null && current.getValue().hasNextX();
    }

    /** */
    private void closeForPart(int partId) throws IgniteCheckedException {
        if (current != null && current.getKey() == partId)
            advance(true);

        GridCloseableIterator<CacheDataRow> partIterator = fullIterators.remove(partId);
        doneParts.remove(partId);

        if (partIterator != null)
            partIterator.close();
    }

    /** {@inheritDoc} */
    @Override public synchronized void replaceFullPrtitions(Map<Integer, GridCloseableIterator<CacheDataRow>> partIters)
        throws IgniteCheckedException {
        assert !closed : "Closed iterator.";

        for (Integer part : partIters.keySet()) {
            if (!fullIterators.containsKey(part))
                fullIterators.put(part, partIters.get(part));
        }

        ArrayList<Integer> partToRemove = new ArrayList<>();

        for (Integer part : fullIterators.keySet()) {
            if (!partIters.containsKey(part))
                partToRemove.add(part);
        }

        for (Integer part : partToRemove)
            closeForPart(part);
    }

    /** {@inheritDoc} */
    @Override public synchronized Set<Integer> fullParts() {
        return fullIterators.keySet();
    }

    /** {@inheritDoc} */
    @Override public synchronized void replaceHistorical(IgniteHistoricalIterator historicalIterator) throws IgniteCheckedException {
        assert !closed : "Closed iterator.";

        if (this.historicalIterator != null)
            this.historicalIterator.close();

        this.historicalIterator = historicalIterator;
    }

    /** {@inheritDoc} */
    @Override public synchronized CacheDataRow nextX() throws IgniteCheckedException {
        if (historicalIterator != null && historicalIterator.hasNextX())
            return historicalIterator.nextX();

        if (current == null || !current.getValue().hasNextX())
            throw new NoSuchElementException();

        CacheDataRow result = current.getValue().nextX();

        assert result.partition() == current.getKey();

        advance(false);

        return result;
    }

    /** {@inheritDoc} */
    @Override public synchronized CacheDataRow peek() {
        if (cached == null) {
            if (!hasNext())
                return null;

            cached = next();
        }

        return cached;
    }

    /** {@inheritDoc} */
    @Override public synchronized void removeX() throws IgniteCheckedException {
        throw new UnsupportedOperationException("remove");
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() throws IgniteCheckedException {
        cached = null;

        if (historicalIterator != null)
            historicalIterator.close();

        if (fullIterators != null) {
            for (GridCloseableIterator<CacheDataRow> iter : fullIterators.values())
                iter.close();
        }

        closed = true;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean isClosed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public synchronized Iterator<CacheDataRow> iterator() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasNext() {
        try {
            if (cached != null)
                return true;

            return hasNextX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized CacheDataRow next() {
        try {
            if (cached != null) {
                CacheDataRow res = cached;

                cached = null;

                return res;
            }

            return nextX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void remove() {
        try {
            removeX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
