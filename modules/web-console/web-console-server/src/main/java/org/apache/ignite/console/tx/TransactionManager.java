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

package org.apache.ignite.console.tx;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.NestedTransaction;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.web.errors.Errors.checkDatabaseNotAvailable;
import static org.apache.ignite.console.web.errors.Errors.isDatabaseNotAvailable;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Transactions manager.
 */
@Service
public class TransactionManager {
    /** */
    private static final Logger log = LoggerFactory.getLogger(TransactionManager.class);

    /** */
    private final Ignite ignite;

    /** */
    private final Map<String, Runnable> cachesStarters = new ConcurrentHashMap<>();

    /**
     * @param ignite Ignite.
     */
    @Autowired
    protected TransactionManager(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * @return Current transaction.
     */
    private Transaction tx() {
        return ignite.transactions().tx();
    }

    /**
     * Starts new transaction with the specified concurrency and isolation.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return New transaction.
     */
    private Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        Transaction curTx = tx();

        if (curTx instanceof NestedTransaction)
            return curTx;

        return curTx == null ? ignite.transactions().txStart(concurrency, isolation) : new NestedTransaction(curTx);
    }

    /**
     * Start transaction.
     *
     * @return Transaction.
     */
    private Transaction txStart() {
        return txStart(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param name Starter name.
     * @param starter Caches starter.
     */
    public void registerStarter(String name, Runnable starter) {
        cachesStarters.putIfAbsent(name, starter);

        starter.run();
    }

    /**
     * Recreate all caches by executing all registered starters.
     */
    private synchronized void recreateCaches() {
        cachesStarters.forEach((name, starter) -> {
            if (log.isDebugEnabled())
                log.debug("Creating caches for: {}", name);

            starter.run();
        });
    }

    /**
     * @param action Action to execute.
     * @return Action result.
     */
    private <T> T doInTransaction0(Supplier<T> action) {
        try (Transaction tx = txStart()) {
            T res = action.get();

            tx.commit();

            return res;
        }
    }

    /**
     * Execute action in transaction.
     *
     * @param name Action name.
     * @param action Action to execute.
     * @return Action result.
     */
    public <T> T doInTransaction(String name, Supplier<T> action) {
        if (log.isDebugEnabled())
            log.debug("Executing action: {}", name);

        // For server nodes no need to recreate caches.
        if (!ignite.configuration().isClientMode())
            return doInTransaction0(action);

        // For client nodes:
        //  1. Try to execute action in transaction.
        //  2. If failed, try to recreate caches.
        //  3. If failed with IgniteClientDisconnectedException - await on reconnect future.
        //  4. If all options failed, throw IgniteClientDisconnectedException that will be converted to 503 error.
        try {
            return doInTransaction0(action);
        }
        catch (Throwable e) {
            if (isDatabaseNotAvailable(e)) {
                try {
                    recreateCaches();
                }
                catch (RuntimeException re) {
                    throw checkDatabaseNotAvailable(re);
                }

                if (tx() instanceof NestedTransaction)
                    throw new IllegalStateException("Database connection was lost during transaction");

                return doInTransaction0(action);
            }

            throw e;
        }
    }

    /**
     * Execute action in transaction.
     *
     * @param name Action name.
     * @param action Action to execute.
     */
    public void doInTransaction(String name, Runnable action) {
        doInTransaction(name, () -> {
            action.run();

            return null;
        });
    }

    /**
     * Checks if active transaction in progress.
     *
     * @throws IllegalStateException If active transaction was not found.
     */
    public void checkInTransaction() {
        if (tx() == null)
            throw new IllegalStateException("No active transaction was found");
    }
}
