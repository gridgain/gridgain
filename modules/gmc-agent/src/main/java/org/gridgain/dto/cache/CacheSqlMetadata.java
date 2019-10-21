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

package org.gridgain.dto.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for cache sql metadata.
 */
public class CacheSqlMetadata {
    /** Cache name. */
    private String cacheName;

    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Types. */
    private Collection<String> types = new ArrayList<>();

    /** Key classes. */
    private Map<String, String> keyClasses = new LinkedHashMap<>();

    /** Value classes. */
    private Map<String, String> valClasses = new LinkedHashMap<>();

    /** Fields. */
    private Map<String, Map<String, String>> fields = new LinkedHashMap<>();

    /** Not null fields. */
    private Map<String, Set<String>> notNullFields = new LinkedHashMap<>();

    /** Indexes. */
    private Map<String, List<CacheSqlIndexMetadata>> indexes = new LinkedHashMap<>();

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     * @return @{code This} for method chaining.
     */
    public CacheSqlMetadata setCacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    /**
     * @return Schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     * @return @{code This} for method chaining.
     */
    public CacheSqlMetadata setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    /**
     * @return Table name.
     */
    public String getTableName() {
        return tblName;
    }

    /**
     * @param tblName Table name.
     * @return @{code This} for method chaining.
     */
    public CacheSqlMetadata setTableName(String tblName) {
        this.tblName = tblName;
        return this;
    }

    /**
     * @return Types.
     */
    public Collection<String> getTypes() {
        return types;
    }

    /**
     * @param types Types.
     * @return @{code This} for method chaining.
     */
    public CacheSqlMetadata setTypes(Collection<String> types) {
        this.types = types;
        return this;
    }

    /**
     * @return Key classes.
     */
    public Map<String, String> getKeyClasses() {
        return keyClasses;
    }

    /**
     * @param keyClasses Key classes.
     * @return @{code This} for method chaining.
     */
    public CacheSqlMetadata setKeyClasses(Map<String, String> keyClasses) {
        this.keyClasses = keyClasses;
        return this;
    }

    /**
     * @return Value classes.
     */
    public Map<String, String> getValueClasses() {
        return valClasses;
    }

    /**
     * @param valClasses Value classes.
     * @return @{code This} for method chaining.
     */
    public CacheSqlMetadata setValueClasses(Map<String, String> valClasses) {
        this.valClasses = valClasses;
        return this;
    }

    /**
     * @return Fields.
     */
    public Map<String, Map<String, String>> getFields() {
        return fields;
    }

    /**
     * @return Not null fields.
     */
    public Map<String, Set<String>> getNotNullFields() {
        return notNullFields;
    }

    /**
     * @param notNullFields Not null fields.
     * @return @{code This} for method chaining.
     */
    public CacheSqlMetadata setNotNullFields(Map<String, Set<String>> notNullFields) {
        this.notNullFields = notNullFields;
        return this;
    }

    /**
     * @param fields Fields.
     * @return @{code This} for method chaining.
     */
    public CacheSqlMetadata setFields(Map<String, Map<String, String>> fields) {
        this.fields = fields;
        return this;
    }

    /**
     * @return Indexes.
     */
    public Map<String, List<CacheSqlIndexMetadata>> getIndexes() {
        return indexes;
    }

    /**
     * @param indexes Indexes.
     * @return @{code This} for method chaining.
     */
    public CacheSqlMetadata setIndexes(Map<String, List<CacheSqlIndexMetadata>> indexes) {
        this.indexes = indexes;
        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheSqlMetadata.class, this);
    }
}
