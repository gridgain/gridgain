/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Extending {@link GridFutureAdapter} to rebuild indices.
 */
public class SchemaIndexCacheFuture extends GridFutureAdapter<Void> {
    /** Token for canceling index rebuilding. */
    private final SchemaIndexOperationCancellationToken cancelTok;

    /**
     * Constructor.
     *
     * @param cancelTok Token for canceling index rebuilding.
     */
    public SchemaIndexCacheFuture(SchemaIndexOperationCancellationToken cancelTok) {
        this.cancelTok = cancelTok;
    }

    /**
     * Getting token for canceling index rebuilding.
     *
     * @return Cancellation token.
     */
    public SchemaIndexOperationCancellationToken cancelToken() {
        return cancelTok;
    }
}
