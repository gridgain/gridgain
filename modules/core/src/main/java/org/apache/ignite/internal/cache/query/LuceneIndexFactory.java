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
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for {@link LuceneIndex} implementations.
 */
public interface LuceneIndexFactory {
    /**
     * @param cacheName Cache name.
     * @param type Type name.
     * @return Lucene index.
     * @throws IgniteCheckedException In case of an error in creating the index.
     */
    public LuceneIndex createIndex(
        @Nullable String cacheName,
        GridQueryTypeDescriptor type
    ) throws IgniteCheckedException;
}
