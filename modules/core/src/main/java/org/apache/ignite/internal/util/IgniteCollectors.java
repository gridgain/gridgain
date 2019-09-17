package org.apache.ignite.internal.util;

import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;

/**
 * Custom collectors for {@link java.util.stream.Stream} API.
 */
public class IgniteCollectors {
    /**
     * Empty constructor.
     */
    private IgniteCollectors() { }

    /**
     * Collector of {@link IgniteInternalFuture} inheritors stream to {@link GridCompoundFuture}.
     *
     * @param <F> Type of {@link IgniteInternalFuture} inheritor.
     * @param <T> Result type of inheritor {@link IgniteInternalFuture}.
     * @param <R> Result type of {@link GridCompoundFuture}.
     * @return Compound future that contains all stream futures
     *         and initialized with {@link GridCompoundFuture#markInitialized()}.
     */
    public static <T, R> Collector<? super IgniteInternalFuture,
        GridCompoundFuture<T, R>, GridCompoundFuture<T, R>> toCompoundFuture() {
        final GridCompoundFuture<T, R> res = new GridCompoundFuture<>();

        return Collectors.collectingAndThen(
            Collectors.reducing(
                res,
                res::add,
                (a, b) -> a // No needs to merge compound futures.
            ),
            GridCompoundFuture::markInitialized
        );
    }
}
