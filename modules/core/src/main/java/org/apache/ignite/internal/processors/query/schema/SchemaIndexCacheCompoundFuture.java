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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.util.future.GridCompoundFuture;

/**
 * Compound index rebuilding feature.
 * Waits for all internal features to complete, even if they throw exceptions.
 * In this case, {@link #error()} will return the first thrown exception.
 */
public class SchemaIndexCacheCompoundFuture extends GridCompoundFuture<SchemaIndexCacheStat, SchemaIndexCacheStat> {
    /** Container for the first index rebuild error. */
    private final AtomicReference<Throwable> errRef = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected boolean ignoreFailure(Throwable err) {
        errRef.compareAndSet(null, err);

        return true;
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        Throwable err0 = super.error();
        Throwable err1 = errRef.get();

        if (err0 != null && err1 != null)
            err0.addSuppressed(err1);

        return err0 != null ? err0 : err1;
    }
}
