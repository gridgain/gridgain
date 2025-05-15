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
 * Ignite supports vector queries based on Apache Lucene engine.
 * Note that all fields that are expected to show up in vector query results must be annotated with {@link QueryVectorField}
 * and be of type {@code float[]}.
 *
 * <h2 class="header">Query usage</h2>
 * As an example, suppose we have data model consisting of {@code 'Person'} class defined as follows:
 * <pre name="code" class="java">
 * public class Person {
 *     private long id;
 *
 *     private String name;
 *
 *     // Index for vector search.
 *     &#64;QueryVectorField
 *     private float[] resume;
 *     ...
 * }
 * </pre>
 *
 * Here is a possible query that will use Lucene vector search to scan all resumes to
 * check if employees have {@code Master} degree:
 * <pre name="code" class="java">
 * Query&lt;Cache.Entry&lt;Long, Person&gt;&gt; qry =
 *     new VectorQuery(Person.class, "resume", toEmbedding("Master"), 1);
 *
 * // Query all cache nodes.
 * cache.query(qry).getAll();
 *
 * Where {@code toEmbedding} is a function that converts text to vector with the transformer
 * and 1 is the number of vectors to return.
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

    /** Query vector. */
    private float[] clauseVector;

    /** [K]NN, how many vectors to return. */
    private int k;

    /** threshold, the threshold for cosine similarity. */
    private float threshold = -1;

    /**
     * Constructs query for the given search vector.
     *
     * @param type Type.
     * @param field Type.
     * @param clauseVector Search vector.
     * @param k The number of vectors to return.
     */
    public VectorQuery(Class<?> type, String field, float[] clauseVector, int k) {
        setType(type);
        setField(field);
        setClauseVector(clauseVector);
        setK(k);
    }

    /**
     * Constructs query for the given search vector.
     *
     * @param type Type.
     * @param field Type.
     * @param clauseVector Search vector.
     * @param k The number of vectors to return.
     * @param threshold The threshold for similarity.
     */
    public VectorQuery(Class<?> type, String field, float[] clauseVector, int k, float threshold) {
        setType(type);
        setField(field);
        setClauseVector(clauseVector);
        setK(k);
        setThreshold(threshold);
    }

    /**
     * Constructs query for the given search string.
     *
     * @param type Type.
     * @param field Type.
     * @param clauseVector Search string.
     * @param k The number of vectors to return.
     */
    public VectorQuery(String type, String field, float[] clauseVector, int k) {
        setType(type);
        setField(field);
        setClauseVector(clauseVector);
        setK(k);
    }

    /**
     * Constructs query for the given search string.
     *
     * @param type Type.
     * @param field Type.
     * @param clauseVector Search string.
     * @param k The number of vectors to return.
     * @param threshold The threshold for similarity.
     */
    public VectorQuery(String type, String field, float[] clauseVector, int k, float threshold) {
        setType(type);
        setField(field);
        setClauseVector(clauseVector);
        setK(k);
        setThreshold(threshold);
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
    public VectorQuery<K, V> setType(String type) {
        this.type = type;

        return this;
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
     * Sets k (the number of vectors to return).
     *
     * @param k K.
     * @return {@code this} For chaining.
     */
    public VectorQuery<K, V> setK(int k) {
        this.k = k;

        return this;
    }

    /**
     * Gets k (the number of vectors to return).
     *
     * @return K.
     */
    public int getK() {
        return k;
    }

    /**
     * Sets threshold for cosine similarity.
     *
     * @param threshold threshold.
     * @return {@code this} For chaining.
     */
    public VectorQuery<K, V> setThreshold(float threshold) {
        this.threshold = threshold;

        return this;
    }

    /**
     * Gets threshold for cosine similarity.
     *
     * @return K.
     */
    public float getThreshold() {
        return threshold;
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
     * Gets search vector.
     *
     * @return Search vector.
     */
    public float[] getClauseVector() {
        return clauseVector;
    }

    /**
     * Sets text search vector.
     *
     * @param clauseVector Text search string as vector.
     * @return {@code this} For chaining.
     */
    public VectorQuery<K, V> setClauseVector(float[] clauseVector) {
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
