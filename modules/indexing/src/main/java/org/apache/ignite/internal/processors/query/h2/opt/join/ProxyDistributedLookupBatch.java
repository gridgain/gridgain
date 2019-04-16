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

import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.h2.index.Cursor;
import org.h2.index.IndexLookupBatch;
import org.h2.result.SearchRow;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Lookip batch for proxy indexes.
 */
public class ProxyDistributedLookupBatch implements IndexLookupBatch {
    /** Underlying normal lookup batch */
    private final IndexLookupBatch delegate;

    /** Row descriptor. */
    private final GridH2RowDescriptor rowDesc;

    /**
     * Creates proxy lookup batch.
     *
     * @param delegate Underlying index lookup batch.
     * @param rowDesc Row descriptor.
     */
    public ProxyDistributedLookupBatch(IndexLookupBatch delegate, GridH2RowDescriptor rowDesc) {
        this.delegate = delegate;
        this.rowDesc = rowDesc;
    }

    /** {@inheritDoc} */
    @Override public boolean addSearchRows(SearchRow first, SearchRow last) {
        SearchRow firstProxy = rowDesc.prepareProxyIndexRow(first);
        SearchRow lastProxy = rowDesc.prepareProxyIndexRow(last);

        return delegate.addSearchRows(firstProxy, lastProxy);
    }

    /** {@inheritDoc} */
    @Override public boolean isBatchFull() {
        return delegate.isBatchFull();
    }

    /** {@inheritDoc} */
    @Override public List<Future<Cursor>> find() {
        return delegate.find();
    }

    /** {@inheritDoc} */
    @Override public String getPlanSQL() {
        return delegate.getPlanSQL();
    }

    /** {@inheritDoc} */
    @Override public void reset(boolean beforeQuery) {
        delegate.reset(beforeQuery);
    }
}
