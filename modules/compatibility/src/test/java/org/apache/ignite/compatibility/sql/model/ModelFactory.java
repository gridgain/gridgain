/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.compatibility.sql.model;

import org.apache.ignite.cache.QueryEntity;

/**
 * Factory for model.
 */
public interface ModelFactory<T> {
    /** Inits factory with a random seed. */
    public void init(int seed);

    /** Creates random model objects. */
    public T createRandom();

    /** Returns model's query entity. */
    public QueryEntity queryEntity();

    /** Returns model's table name. */
    public String tableName();

    /** Returns number of model instances. */
    public int count();
}
