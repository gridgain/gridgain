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
package org.apache.ignite.internal.sql.optimizer.affinity;

import org.jetbrains.annotations.Nullable;

/**
 * Client context. Passed to partition resolver on thin clients.
 */
public class PartitionClientContext {
    /**
     * Resolve partition.
     *
     * @param arg Argument.
     * @param typ Type.
     * @param cacheName Cache name.
     * @return Partition or {@code null} if cannot be resolved.
     */
    @Nullable public Integer partition(Object arg, @Nullable PartitionParameterType typ, String cacheName) {
        PartitionDataTypeUtils.convert(arg, typ);

        // TODO: IGNITE-10308: Implement partition resolution logic.
        return null;
    }
}
