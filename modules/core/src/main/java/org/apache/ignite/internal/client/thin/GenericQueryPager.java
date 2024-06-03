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

import java.util.Collection;
import java.util.function.Consumer;

import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientReconnectedException;

/**
 * Generic query pager. Override {@link this#readResult(PayloadInputChannel, boolean)} to make it specific.
 */
abstract class GenericQueryPager<T> implements QueryPager<T> {
    /** Query op. */
    private final ClientOperation qryOp;

    /** Query op. */
    private final ClientOperation pageQryOp;

    /** Query writer. */
    private final Consumer<PayloadOutputChannel> qryWriter;

    /** Channel. */
    private final ReliableChannel ch;

    /** Has next. */
    private boolean hasNext = true;

    /** Initial query response. */
    private Collection<T> firstPage;

    /** Cursor id. */
    private Long cursorId = null;

    /** Client channel on first query page. */
    private ClientChannel clientCh;

    /** Constructor. */
    GenericQueryPager(
        ReliableChannel ch,
        ClientOperation qryOp,
        ClientOperation pageQryOp,
        Consumer<PayloadOutputChannel> qryWriter
    ) {
        this.ch = ch;
        this.qryOp = qryOp;
        this.pageQryOp = pageQryOp;
        this.qryWriter = qryWriter;
    }

    public void loadFirstPage() {
        firstPage = ch.service(qryOp, qryWriter, (PayloadInputChannel payloadCh) -> readResult(payloadCh, true));
    }

    /** {@inheritDoc} */
    @Override public Collection<T> next() throws ClientException {
        if (!hasNext)
            throw new IllegalStateException("No more query results");

        if (cursorId == null) {
            loadFirstPage();
            Collection<T> res = firstPage;
            firstPage = null;

            return res;
        }

        return queryPage();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        // Close cursor only if the server has more pages: the server closes cursor automatically on last page
        if (cursorId != null && hasNext && !clientCh.closed()) {
            try {
                clientCh.service(ClientOperation.RESOURCE_CLOSE, req -> req.out().writeLong(cursorId), null);
            } catch (ClientConnectionException | ClientReconnectedException ignored) {
                // Original connection was lost and cursor was closed by the server.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return hasNext;
    }

    /** {@inheritDoc} */
    @Override public boolean hasFirstPage() {
        return cursorId != null;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        firstPage = null;

        hasNext = true;

        cursorId = null;

        clientCh = null;
    }

    /**
     * Override this method to read entries from the input stream. "Entries" means response data excluding heading
     * cursor ID and trailing "has next page" flag.
     */
    abstract Collection<T> readEntries(PayloadInputChannel in, boolean firstPage);

    /** */
    private Collection<T> readResult(PayloadInputChannel payloadCh, boolean firstPage) {
        if (firstPage) {
            long resCursorId = payloadCh.in().readLong();

            if (cursorId != null) {
                if (cursorId != resCursorId)
                    throw new ClientProtocolError(
                        String.format("Expected cursor [%s] but received cursor [%s]", cursorId, resCursorId)
                    );
            }
            else {
                cursorId = resCursorId;

                clientCh = payloadCh.clientChannel();
            }
        }

        Collection<T> res = readEntries(payloadCh, firstPage);

        hasNext = payloadCh.in().readBoolean();

        return res;
    }

    /** Get page. */
    private Collection<T> queryPage() throws ClientException {
        return clientCh.service(
                pageQryOp,
                req -> req.out().writeLong(cursorId),
                (PayloadInputChannel payloadCh) -> readResult(payloadCh, false));
    }
}
