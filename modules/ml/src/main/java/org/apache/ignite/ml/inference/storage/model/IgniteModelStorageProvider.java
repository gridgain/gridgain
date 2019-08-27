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

package org.apache.ignite.ml.inference.storage.model;

import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Implementation of {@link ModelStorageProvider} based on Apache Ignite cache.
 */
public class IgniteModelStorageProvider implements ModelStorageProvider {
    /** Ignite instane to start transactions. */
    private final Ignite ignite;

    /** Storage of the files and directories. */
    private final IgniteCache<String, FileOrDirectory> cache;

    /**
     * Constructs a new instance of Ignite model storage provider.
     *
     * @param cache Storage of the files and directories.
     */
    public IgniteModelStorageProvider(Ignite ignite, IgniteCache<String, FileOrDirectory> cache) {
        this.ignite = ignite;
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public FileOrDirectory get(String path) {
        return cache.get(path);
    }

    /** {@inheritDoc} */
    @Override public void put(String path, FileOrDirectory file) {
        cache.put(path, file);
    }

    /** {@inheritDoc} */
    @Override public void remove(String path) {
        cache.remove(path);
    }

    /** {@inheritDoc} */
    @Override public <T> T synchronize(Supplier<T> supplier) {
        Transaction tx0 = ignite.transactions().tx();

        if (tx0 != null)
            return supplier.get();
        else {
            try {
                return CU.retryTopologySafe(() -> {
                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        T res = supplier.get();

                        tx.commit();

                        return res;

                    }
                });
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
