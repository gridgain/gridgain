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

package org.apache.ignite.cache.query.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;

/**
 * Annotates classes for SQL queries. This annotation is only needed if the
 * default properties need to be overridden. It can only be set for the
 * value class. Throws @{link CacheException} when used on the key. For more
 * information about cache queries see {@link CacheQuery} documentation.
 * @see CacheQuery
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface QuerySqlTable {
    /**
     * Table name. Set the name of the table in SQL. Defaults to the class
     * name of the value.
     *
     * @return Table name
     */
    String name() default "";

    /**
     * Key name. Can be used to denote the key as a whole, for example, if the
     * key is a scalar.
     *
     * @return Key name
     */
    String keyFieldName() default "";

    /**
     * Value name. Cam be used to denote the value as a whole, for example, if
     * the value is a scalar.
     *
     * @return Value name
     */
    String valueFieldName() default "";
}
