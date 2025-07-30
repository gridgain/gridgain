/**
 * Copyright (c) 2023-2024 Hewlett-Packard Enterprise Development LP.
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of
 * Hewlett-Packard Enterprise Development LP.
 */

package org.apache.ignite.internal.benchmarks.trafgen.utils;

/**
 * Represents an operation that accepts a single input argument and returns no result. Unlike most other functional
 * interfaces, {@code Consumer} is expected to operate via side-effects.
 *
 * <p>
 * This is a <a href="package-summary.html">functional interface</a> whose functional method is
 * {@link #accept(Object)}.
 *
 * @param <T>
 *            the type of the input to the operation
 * @param <E>
 *            the type of the exception
 *
 * @since 1.8
 */
@FunctionalInterface
public interface ThrowingConsumer<T, E extends Throwable>
{

    /**
     * Performs this operation on the given argument.
     *
     * @param t
     *            the input argument
     * @throws E
     *             an exception if there is an issue
     */
    void accept(T t) throws E;
}
