/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.tracing;

import java.io.ObjectInputStream;
import java.io.Serializable;

import java.util.Objects;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Specifies to which traces, specific configuration will be applied. In other words it's a sort of tracing
 * configuration locator.
 */
public class TracingConfigurationCoordinates implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /**
     * Specifies the {@link Scope} of a trace's root span to which some specific tracing configuration will be applied.
     * It's a mandatory attribute.
     */
    private Scope scope;

    /**
     * Specifies the label of a traced operation. It's an optional attribute.
     */
    private String lb;

    /**
     * Private constructor to be used with builder.
     *
     * @param scope scope Specifies the {@link Scope} of a trace's root span to which some specific
     *  tracing configuration will be applied.
     * @param lb Specifies the label of a traced operation.
     */
    private TracingConfigurationCoordinates(@NotNull Scope scope, @Nullable String lb) {
        this.scope = scope;
        this.lb = lb;
    }

    /**
     * @return {@link Scope} of a trace's root span to which some specific tracing configuration will be applied.
     */
    public Scope scope() {
        return scope;
    }

    /**
     * @return Label of a traced operation, to which some specific tracing configuration will be applied.
     */
    @Nullable public String label() {
        return lb;
    }

    /**
     * Since most of the time we can expect nullable labels (or labels with same value)
     * we can reuse existing coordinates instances.
     * @param label Label.
     * @return Coordinates.
     */
    public TracingConfigurationCoordinates withLabel(@Nullable String label) {
        if (Objects.equals(lb, label))
            return this;

        return new TracingConfigurationCoordinates.Builder(scope).withLabel(label).build();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TracingConfigurationCoordinates that = (TracingConfigurationCoordinates)o;

        if (scope != that.scope)
            return false;

        return lb != null ? lb.equals(that.lb) : that.lb == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = scope != null ? scope.hashCode() : 0;

        res = 31 * res + (lb != null ? lb.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TracingConfigurationCoordinates.class, this);
    }

    /**
     * {@code TracingConfigurationCoordinates} builder.
     */
    @SuppressWarnings("PublicInnerClass") public static class Builder {
        /** Counterpart of {@code TracingConfigurationCoordinator}'s scope. */
        private final Scope scope;

        /** Counterpart of {@code TracingConfigurationCoordinator}'s lb. */
        private String lb;

        /**
         * Constructor.
         *
         * @param scope Mandatory scope attribute.
         * @throws IllegalArgumentException if null scope is specified.
         */
        public Builder(Scope scope) {
            if (scope == null)
                throw new IllegalArgumentException("Null scope is not valid for tracing coordinates.");

            this.scope = scope;
        }

        /**
         * Builder method that allows to set optional label attribute.
         *
         * @param lb Label of traced operation. It's an optional attribute.
         * @return Current {@code TracingConfigurationCoordinates} instance.
         */
        public @NotNull Builder withLabel(@Nullable String lb) {
            this.lb = lb;

            return this;
        }

        /**
         * Builder's build() method.
         *
         * @return {@code TracingConfigurationCoordinates} instance.
         */
        public TracingConfigurationCoordinates build() {
            return new TracingConfigurationCoordinates(scope, lb);
        }
    }

    /**
     * Deserialize tracing configuration coordinated.
     * @param in Input stream.
     */
    private void readObject(ObjectInputStream in) {
        try {
            in.defaultReadObject();
        }
        catch (Exception e) {
            LT.warn(log, "Unable to deserialize tracing configuration coordinates: " + e.getMessage());

            scope = null;
            lb = null;
        }
    }
}
