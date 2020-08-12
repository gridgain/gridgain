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

package org.apache.ignite.examples.ml.util;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;

/**
 * Represents an operation that accepts a single {@code double}-valued argument and
 * returns no result.  This is the primitive type specialization of
 * {@link Consumer} for {@code double}.  Unlike most other functional interfaces,
 * {@code DoubleConsumer} is expected to operate via side-effects.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(double)}.
 */
@FunctionalInterface
public interface SerializableDoubleConsumer extends Serializable, DoubleConsumer {
}
