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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Map;
import java.util.Collection;
import java.util.HashMap;

/**
 * Index descriptor.
 */
public class QueryEntityVectorIndexDescriptor extends QueryEntityIndexDescriptor {
    /** Type descriptor. */

    private Map<String, Integer> fields = new HashMap<>();

    /**
     * Constructor.
     *
     */
    public QueryEntityVectorIndexDescriptor() {
        super(QueryIndexType.VECTOR);
    }


    /** {@inheritDoc} */
    @Override public String name() {
        return null;
    }

    @Override
    public Collection<String> fields() {
        return fields.keySet();
    }

    @Override
    public boolean descending(String field) {
        return false;
    }


    /**
     * Adds field to this index.
     *
     * @param field Field name.
     * @param similarityFunction Field order number in this index.
     * @return This instance for chaining.
     * @throws IgniteCheckedException If failed.
     */
    public void addField(String field, int similarityFunction) {
        fields.put(field, similarityFunction);
    }


    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntityVectorIndexDescriptor.class, this);
    }
}
