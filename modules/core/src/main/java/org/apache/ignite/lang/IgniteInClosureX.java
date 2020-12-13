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

package org.apache.ignite.lang;

import org.apache.ignite.IgniteCheckedException;

/**
 * Closure with one in-parameter and void return type that allows for thrown grid exception.
 *
 * @param <E> Type of closure argument.
 */
public interface IgniteInClosureX<E> {
    /**
     * Closure body.
     *
     * @param e Closure argument.
     * @throws IgniteCheckedException Thrown in case of any error condition inside of the closure.
     */
    void applyx(E e) throws IgniteCheckedException;
}
