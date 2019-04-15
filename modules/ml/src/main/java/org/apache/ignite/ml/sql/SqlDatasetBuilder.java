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

package org.apache.ignite.ml.sql;

import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDataset;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;

/**
 * Subclass of {@link CacheBasedDatasetBuilder} that should be used to work with SQL tables.
 */
public class SqlDatasetBuilder extends CacheBasedDatasetBuilder<Object, BinaryObject> {
    /**
     * Constructs a new instance of cache based dataset builder that makes {@link CacheBasedDataset} with default
     * predicate that passes all upstream entries to dataset.
     *
     * @param ignite Ignite instance.
     * @param upstreamCache Name of Ignite Cache with {@code upstream} data.
     */
    public SqlDatasetBuilder(Ignite ignite, String upstreamCache) {
        this(ignite, upstreamCache, (a, b) -> true);
    }

    /**
     * Constructs a new instance of cache based dataset builder that makes {@link CacheBasedDataset}.
     *
     * @param ignite Ignite instance.
     * @param upstreamCache Name of Ignite Cache with {@code upstream} data.
     * @param filter Filter for {@code upstream} data.
     */
    public SqlDatasetBuilder(Ignite ignite, String upstreamCache, IgniteBiPredicate<Object, BinaryObject> filter) {
        this(ignite, upstreamCache, filter, UpstreamTransformerBuilder.identity());
    }

    /**
     * Constructs a new instance of cache based dataset builder that makes {@link CacheBasedDataset}.
     *
     * @param ignite Ignite instance.
     * @param upstreamCache Name of Ignite Cache with {@code upstream} data.
     * @param filter Filter for {@code upstream} data.
     */
    public SqlDatasetBuilder(Ignite ignite, String upstreamCache, IgniteBiPredicate<Object, BinaryObject> filter,
        UpstreamTransformerBuilder transformerBuilder) {
        super(ignite, ignite.cache(upstreamCache), filter, transformerBuilder, true);
    }
}
