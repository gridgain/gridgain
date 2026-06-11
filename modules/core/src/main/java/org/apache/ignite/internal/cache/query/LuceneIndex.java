/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.cache.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;

/**
 * Lucene index.
 */
public interface LuceneIndex extends AutoCloseable {
    /**
     * Stores given data in this index.
     *
     * @param k Key.
     * @param v Value.
     * @param ver Version.
     * @param expires Expiration time.
     * @param link Data-row link of the entry ({@code 0} if not available) — lets the index
     *     materialize results by a direct row read instead of duplicating key/value bytes.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void store(CacheObject k, CacheObject v, GridCacheVersion ver, long expires, long link)
        throws IgniteCheckedException;

    /**
     * Removes entry for given key from this index.
     *
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(CacheObject key) throws IgniteCheckedException;

    /**
     * Runs lucene fulltext query over this index.
     *
     * @param qry Query.
     * @param filters Filters over result.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> textQuery(String qry, IndexingQueryFilter filters)
        throws IgniteCheckedException;

    /**
     * Runs lucene vector query over this index.
     *
     * @param qryVector Query as vector.
     * @param k Number of nearest neighbours to return.
     * @param threshold Minimum similarity score, or a negative value for no threshold.
     * @param efSearch Search-time beam width, or {@code 0} for the engine default.
     * @param filters Filters over result.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> vectorQuery(String field, float[] qryVector,
        int k, float threshold, int efSearch, IndexingQueryFilter filters) throws IgniteCheckedException;

    /**
     * Destroys the index: like {@link #close()}, but additionally drops any persistent
     * state the index keeps. Called when the cache is destroyed (as opposed to a node
     * stop / cache close, where persistent index state must survive).
     */
    public default void destroy() throws Exception {
        close();
    }

    /**
     * Asks the index whether it needs the row-scan rebuild after a restart. An index
     * that restored itself from its own persistent state (e.g. a vector graph
     * snapshot) returns {@code false} and the scan is skipped.
     *
     * @return {@code True} if the index must be rebuilt from cache rows.
     */
    public default boolean needsRebuild() {
        return true;
    }

    /**
     * Invoked early in the node-stop sequence, before cache stop and before the
     * database manager blocks checkpoint-lock acquisition — the last point where an
     * index can still write persistent state (e.g. its parting graph snapshot) that
     * the final checkpoint of a graceful stop will flush.
     */
    public default void beforeNodeStop() {
        // No-op.
    }
}
