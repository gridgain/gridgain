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

package org.apache.ignite.cache.query;

import org.apache.ignite.cache.query.exceptions.SqlMemoryQuotaExceededException;
import org.apache.ignite.transactions.TransactionException;

import javax.cache.CacheException;
import java.util.List;

/**
 * Query result cursor. Implements {@link Iterable} only for convenience, e.g. {@link #iterator()}
 * can be obtained only once. Also if iteration is started then {@link #getAll()} method calls are prohibited.
 * <p/>
 * During execution, {@link #getAll()} and {@link #iterator()} may throw exceptions:
 * <ul>
 *   <li>{@link SqlMemoryQuotaExceededException} if the query memory quota is exceeded</li>
 *   <li>{@link CacheException} if an error occurs, possibly caused by:
 *       <ul>
 *           <li>{@link TransactionException}</li>
 *           <li>{@link QueryRetryException}</li>
 *           <li>{@link QueryCancelledException}</li>
 *       </ul>
 *   </li>
 * </ul>
 */
public interface QueryCursor<T> extends Iterable<T>, AutoCloseable {
    /**
     * Gets all query results and stores them in the collection.
     * Use this method when you know in advance that query result is
     * relatively small and will not cause memory utilization issues.
     * <p>
     * Since all the results will be fetched, all the resources will be closed
     * automatically after this call, e.g. there is no need to call {@link #close()} method in this case.
     *
     * @return List containing all query results.
     */
    public List<T> getAll();

    /**
     * Closes all resources related to this cursor. If the query execution is in progress
     * (which is possible in case of invoking from another thread), a cancel will be attempted.
     * Sequential calls to this method have no effect.
     * <p>
     * Note: don't forget to close query cursors. Not doing so may lead to various resource leaks.
     */
    @Override public void close();
}