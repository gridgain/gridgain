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

import java.util.Random;
import org.apache.ignite.cache.QueryEntity;

/**
 * Abstract factory for model.
 */
public abstract class AbstractModelFactory<T> implements ModelFactory<T> {
    /** */
    protected final QueryEntity qryEntity;

    /** */
    protected Random rnd;

    /**
     * @param qryEntity Query entity.
     */
    protected AbstractModelFactory(QueryEntity qryEntity) {
        this.qryEntity = qryEntity;

        rnd= new Random();
    }

    /** {@inheritDoc} */
    @Override public void init(int seed) {
        rnd = new Random(seed);
    }

    /** {@inheritDoc} */
    @Override public QueryEntity queryEntity() {
        return qryEntity;
    }
}
