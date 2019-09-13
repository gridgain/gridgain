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

package org.apache.ignite.cache;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.jetbrains.annotations.Nullable;

/**
 * Cache entry along with version information.
 */
public interface CacheInterceptorBinaryEntry<K, V> {
    /**
     * Gets entry's key.
     *
     * @return Entry's key. Returns the de-serialized key object or BinaryObject if the initial user operation
     *      is executed with the flag {@code keepBinary=true}, e.g. see {@link IgniteCache#withKeepBinary()}
     */
    public K key();

    /**
     * Checks the key is binary object.
     * Returns {@code false} is the key type is:
     *  <ul>
     *         <li>any of primitive type: boolean, byte, char, int, long, double, float, and all wrappers
     *         (Boolean, Byte, etc...); </li>
     *         <li>array of any type;</li>
     *         <li>{@link String};</li>
     *         <li>{@link UUID};</li>
     *         <li>{@link Date}, {@link java.sql.Date}, {@link java.sql.Time} and {@link java.sql.Timestamp};</li>
     *         <li>{@link BigDecimal};</li>
     *         <li>any {@link Collection} implementations;</li>
     *         <li>any {@link Map} implementations;</li>
     *  </ul>
     *
     * @return {@code true} if the key is binary object. Otherwise (key's type is a platform type) returns false.
     */
    public boolean isKeyBinary();

    /**
     * Gets entry's key as binary object without unmarshaling.
     *
     * @return Entry's key as binary object.
     * @throws IgniteException if the key is not binary (see more {@link #isKeyBinary()}).
     */
    public BinaryObject keyBinary();

    /**
     * Gets entry's value.
     *
     * @return Entry's value. Returns the de-serialized key object or BinaryObject if the initial user operation
     *      is executed with the flag {@code keepBinary=true}, e.g. see {@link IgniteCache#withKeepBinary()}
     */
    public V value();

    /**
     * Checks the value is binary object.
     * Returns {@code false} is the value type is:
     *  <ul>
     *         <li>any of primitive type: boolean, byte, char, int, long, double, float, and all wrappers
     *         (Boolean, Byte, etc...) </li>
     *         <li>array of any type;</li>
     *         <li>{@link String};</li>
     *         <li>{@link UUID};</li>
     *         <li>{@link Date}, {@link java.sql.Date}, {@link java.sql.Time} and {@link java.sql.Timestamp};</li>
     *         <li>{@link BigDecimal};</li>
     *         <li>any {@link Collection} implementations;</li>
     *         <li>any {@link Map} implementations;</li>
     *  </ul>
     *
     * @return {@code true} if the value is binary object. Otherwise (value's type is a platform type) returns false.
     */
    public boolean isValueBinary();

    /**
     * Gets entry's value object without unmarshaling.
     *
     * @return Entry's value as binary object.
     * @throws IgniteException if the value is not binary (see more {@link #isValueBinary()}).
     */
    public @Nullable BinaryObject valueBinary();
}