/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.opt.join;

import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.h2.index.Cursor;
import org.h2.result.Row;
import org.h2.result.SearchRow;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

/**
 * Merge cursor from multiple nodes.
 */
@SuppressWarnings("ComparatorNotSerializable")
public class BroadcastCursor implements Cursor, Comparator<RangeStream> {
    /** Index. */
    private final GridH2IndexBase idx;

    /** */
    private final int rangeId;

    /** */
    private final RangeStream[] streams;

    /** */
    private boolean first = true;

    /** */
    private int off;

    /**
     * @param rangeId Range ID.
     * @param segmentKeys Remote nodes.
     * @param rangeStreams Range streams.
     */
    public BroadcastCursor(GridH2IndexBase idx, int rangeId, Collection<SegmentKey> segmentKeys,
        Map<SegmentKey, RangeStream> rangeStreams) {
        this.idx = idx;
        this.rangeId = rangeId;

        streams = new RangeStream[segmentKeys.size()];

        int i = 0;

        for (SegmentKey segmentKey : segmentKeys) {
            RangeStream stream = rangeStreams.get(segmentKey);

            assert stream != null;

            streams[i++] = stream;
        }
    }

    /** {@inheritDoc} */
    @Override public int compare(RangeStream o1, RangeStream o2) {
        if (o1 == o2)
            return 0;

        // Nulls are at the beginning of array.
        if (o1 == null)
            return -1;

        if (o2 == null)
            return 1;

        return idx.compareRows(o1.get(rangeId), o2.get(rangeId));
    }

    /**
     * Try to fetch the first row.
     *
     * @return {@code true} If we were able to find at least one row.
     */
    private boolean goFirst() {
        // Fetch first row from all the streams and sort them.
        for (int i = 0; i < streams.length; i++) {
            if (!streams[i].next(rangeId)) {
                streams[i] = null;
                off++; // After sorting this offset will cut off all null elements at the beginning of array.
            }
        }

        if (off == streams.length)
            return false;

        Arrays.sort(streams, this);

        return true;
    }

    /**
     * Fetch next row.
     *
     * @return {@code true} If we were able to find at least one row.
     */
    private boolean goNext() {
        assert off != streams.length;

        if (!streams[off].next(rangeId)) {
            // Next row from current min stream was not found -> nullify that stream and bump offset forward.
            streams[off] = null;

            return ++off != streams.length;
        }

        // Bubble up current min stream with respect to fetched row to achieve correct sort order of streams.
        H2Utils.bubbleUp(streams, off, this);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        if (first) {
            first = false;

            return goFirst();
        }

        return goNext();
    }

    /** {@inheritDoc} */
    @Override public Row get() {
        return streams[off].get(rangeId);
    }

    /** {@inheritDoc} */
    @Override public SearchRow getSearchRow() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean previous() {
        throw new UnsupportedOperationException();
    }
}
