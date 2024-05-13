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

import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QueryVectorField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Vector queries based on Apache Lucene engine.
 * <h1 class="header">Description</h1>
 * Ignite supports cector queries based on Apache Lucene engine.
 * Note that all fields that are expected to show up in vector query results must be annotated with {@link QueryVectorField}
 *
 * <h2 class="header">Query usage</h2>
 * As an example, suppose we have data model consisting of {@code 'Employee'} class defined as follows:
 * <pre name="code" class="java">
 * public class Person {
 *     private long id;
 *
 *     private String name;
 *
 *     // Index for text search.
 *     &#64;QueryVectorField
 *     private String resume;
 *     ...
 * }
 * </pre>
 *
 * Here is a possible query that will use Lucene vector search to scan all resumes to
 * check if employees have {@code Master} degree:
 * <pre name="code" class="java">
 * Query&lt;Cache.Entry&lt;Long, Person&gt;&gt; qry =
 *     new Vector(Person.class, "resume", "Master");
 *
 * // Query all cache nodes.
 * cache.query(qry).getAll();
 * </pre>
 *
 * @see IgniteCache#query(Query)
 */
public final class VectorQuery<K, V> extends Query<Cache.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String type;

    /** */
    private String field;

    /** SQL clause. */
    private String cause;

    /** SQL clause as vector. */
    private float[] clauseVector;

    /**
     * Constructs query for the given search string.
     *
     * @param type Type.
     * @param field Type.
     * @param cause Search string.
     */
    public VectorQuery(Class<?> type, String field, String cause) {
        setType(type);
        setField(field);
        setCause(cause);
    }

    /**
     * Constructs query for the given search string.
     *
     * @param type Type.
     * @param field Type.
     * @param clauseVector Search string.
     */
    public VectorQuery(Class<?> type, String field, float[] clauseVector) {
        setType(type);
        setField(field);
        setCauseVector(clauseVector);
    }

    /**
     * Gets type for query.
     *
     * @return Type.
     */
    public String getType() {
        return type;
    }

    /**
     * Sets type for query.
     *
     * @param type Type.
     * @return {@code this} For chaining.
     */
    public VectorQuery<K, V> setType(Class<?> type) {
        this.type = QueryUtils.typeName(type);

        return this;
    }

    /**
     * Gets field for query.
     *
     * @return Type.
     */
    public String getField() {
        return field;
    }

    /**
     * Sets field for query.
     *
     * @param field field.
     * @return {@code this} For chaining.
     */
    public VectorQuery<K, V> setField(String field) {
        this.field = field;

        return this;
    }

    /**
     * Gets text search string.
     *
     * @return Text search string.
     */
    public String getCause() {
        return cause;
    }

    /**
     * Sets text search string.
     *
     * @param cause Text search string.
     * @return {@code this} For chaining.
     */
    public VectorQuery<K, V> setCause(String cause) {
        A.notNull(cause, "cause");

        this.cause = cause;

        return this;
    }

    /**
     * Gets text search string.
     *
     * @return Text search string as vector.
     */
    public float[] getCauseVector() {
        return clauseVector;
    }

    /**
     * Sets text search string as vector.
     *
     * @param clauseVector Text search string as vector.
     * @return {@code this} For chaining.
     */
    public VectorQuery<K, V> setCauseVector(float[] clauseVector) {
        A.notNull(clauseVector, "clauseVector");

        this.clauseVector = clauseVector;

        return this;
    }

    /** {@inheritDoc} */
    @Override public VectorQuery<K, V> setPageSize(int pageSize) {
        return (VectorQuery<K, V>)super.setPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override public VectorQuery<K, V> setLocal(boolean loc) {
        return (VectorQuery<K, V>)super.setLocal(loc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VectorQuery.class, this);
    }
}
