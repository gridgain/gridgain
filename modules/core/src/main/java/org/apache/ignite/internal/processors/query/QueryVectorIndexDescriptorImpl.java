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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Map;
import java.util.Collection;
import java.util.HashMap;

/**
 * Index descriptor.
 */
public class QueryVectorIndexDescriptorImpl implements GridQueryIndexDescriptor {

    /** Type descriptor. */
    @GridToStringExclude
    private final QueryTypeDescriptorImpl typDesc;

    /** Index name. */
    private final String name;

    /** */
    private final QueryIndexType type = QueryIndexType.VECTOR;

    private Map<String, Integer> fields = new HashMap<>();

    /**
     * Constructor.
     *
     * @param typDesc Type descriptor.
     * @param name Index name.
     */
    public QueryVectorIndexDescriptorImpl(QueryTypeDescriptorImpl typDesc, String name) {
        assert type != null;
        this.typDesc = typDesc;
        this.name = name;
    }

    /**
     * @return Type descriptor.
     */
    public QueryTypeDescriptorImpl typeDescriptor() {
        return typDesc;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    @Override
    public Collection<String> fields() {
        return fields.keySet();
    }

    @Override
    public boolean descending(String field) {
        return false;
    }

    public int similarity(String field) {
        return fields.get(field);
    }


    /**
     * Adds field to this index.
     *
     * @param field Field name.
     * @param similarityFunction Field order number in this index.
     * @return This instance for chaining.
     * @throws IgniteCheckedException If failed.
     */
    public QueryVectorIndexDescriptorImpl addField(String field, int similarityFunction)
        throws IgniteCheckedException {
        if (!typDesc.hasField(field))
            throw new IgniteCheckedException("Field not found: " + field);
        fields.put(field, similarityFunction);
        return this;
    }

    /** {@inheritDoc} */
    @Override public QueryIndexType type() {
        return type;
    }

    @Override
    public int inlineSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryVectorIndexDescriptorImpl.class, this);
    }
}
