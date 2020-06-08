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

package org.apache.ignite.internal.util.typedef;

import javax.cache.Cache;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Defines {@code alias} for <tt>GridPredicate&lt;Entry&lt;K, V&gt;&gt;</tt> by extending
 * {@link org.apache.ignite.lang.IgnitePredicate}. Since Java doesn't provide type aliases (like Scala, for example) we resort
 * to these types of measures. This is intended to provide for more concise code without sacrificing
 * readability. For more information see {@link org.apache.ignite.lang.IgnitePredicate} and {@link javax.cache.Cache.Entry}.
 * @see org.apache.ignite.lang.IgnitePredicate
 * @see GridFunc
 */
public interface PCE<K, V> extends IgnitePredicate<Cache.Entry<K, V>> { /* No-op. */ }
