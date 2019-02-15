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

package org.apache.ignite.testframework;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.TRANSACTION_SERIALIZATION_ERROR;
import static org.junit.Assert.fail;

/**
 * Provides checks for features supported when FORCE_MVCC mode is on.
 */
public class MvccFeatureChecker {
    /** */
    private static final boolean FORCE_MVCC =
        IgniteSystemProperties.getBoolean(IGNITE_FORCE_MVCC_MODE_IN_TESTS, false);

    /** */
    public enum Feature {
        CACHE_STORE,
        NEAR_CACHE,
        LOCAL_CACHE,
        ENTRY_LOCK,
        CACHE_EVENTS,
        EVICTION,
        EXPIRATION,
        METRICS,
        INTERCEPTOR
    }

    /**
     * Fails if feature is not supported.
     *
     * @param f feature.
     * @throws AssertionError If failed.
     */
    public static void failIfNotSupported(Feature f) {
        if (!forcedMvcc())
            return;

        validateFeature(f);
    }

    /**
     * @return {@code True} if Mvcc mode is forced.
     */
    public static boolean forcedMvcc() {
        return FORCE_MVCC;
    }

    /**
     * Check if feature is supported.
     *
     * @param f Feature.
     * @return {@code True} if feature is supported, {@code False} otherwise.
     */
    public static boolean isSupported(Feature f) {
        try {
            validateFeature(f);

            return true;
        }
        catch (AssertionError ignore) {
            return false;
        }
    }

    /**
     * Check if Tx mode is supported.
     *
     * @param conc Transaction concurrency.
     * @param iso Transaction isolation.
     * @return {@code True} if feature is supported, {@code False} otherwise.
     */
    public static boolean isSupported(TransactionConcurrency conc, TransactionIsolation iso) {
            return conc == TransactionConcurrency.PESSIMISTIC &&
                iso == TransactionIsolation.REPEATABLE_READ;
    }


    /**
     * Check if Cache mode is supported.
     *
     * @param mode Cache mode.
     * @return {@code True} if feature is supported, {@code False} otherwise.
     */
    public static boolean isSupported(CacheMode mode) {
        return mode != CacheMode.LOCAL || isSupported(Feature.LOCAL_CACHE);
    }

    /**
     * TODO proper exception handling after https://issues.apache.org/jira/browse/IGNITE-9470
     * Checks if given exception was caused by MVCC write conflict.
     *
     * @param e Exception.
     */
    public static void assertMvccWriteConflict(Exception e) {
        IgniteSQLException sqlEx = X.cause(e, IgniteSQLException.class);

        if (sqlEx == null ||  sqlEx.statusCode() != TRANSACTION_SERIALIZATION_ERROR)
            fail("Unexpected exception: " + X.getFullStackTrace(e));
    }

    /**
     * Fails if feature is not supported in Mvcc mode.
     *
     * @param feature Mvcc feature.
     * @throws AssertionError If failed.
     */
    @SuppressWarnings("fallthrough")
    private static void validateFeature(Feature feature) {
        switch (feature) {
            case NEAR_CACHE:
                fail("https://issues.apache.org/jira/browse/IGNITE-7187");

            case LOCAL_CACHE:
                fail("https://issues.apache.org/jira/browse/IGNITE-9530");

            case CACHE_STORE:
                fail("https://issues.apache.org/jira/browse/IGNITE-8582");

            case ENTRY_LOCK:
                fail("https://issues.apache.org/jira/browse/IGNITE-9324");

            case CACHE_EVENTS:
                fail("https://issues.apache.org/jira/browse/IGNITE-9321");

            case EVICTION:
                fail("https://issues.apache.org/jira/browse/IGNITE-7956");

            case EXPIRATION:
                fail("https://issues.apache.org/jira/browse/IGNITE-7311");

            case METRICS:
                fail("https://issues.apache.org/jira/browse/IGNITE-9224");

            case INTERCEPTOR:
                fail("https://issues.apache.org/jira/browse/IGNITE-9323");
        }
    }
}
