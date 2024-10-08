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

package org.apache.ignite.failure;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.failure.FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_BLOCKED;

/**
 * Abstract superclass for {@link FailureHandler} implementations.
 * Maintains a set of ignored failure types. Failure handler will not invalidate kernal context for this failures
 * and will not handle it.
 */
public abstract class AbstractFailureHandler implements FailureHandler {
    /** */
    @GridToStringInclude
    private Set<FailureType> ignoredFailureTypes =
            Collections.unmodifiableSet(EnumSet.of(SYSTEM_WORKER_BLOCKED, SYSTEM_CRITICAL_OPERATION_TIMEOUT));

    /**
     * Sets failure types that must be ignored by failure handler.
     *
     * @param failureTypes Set of failure type that must be ignored.
     * @see FailureType
     */
    public void setIgnoredFailureTypes(Set<FailureType> failureTypes) {
        ignoredFailureTypes = Collections.unmodifiableSet(failureTypes);
    }

    /**
     * Returns unmodifiable set of ignored failure types.
     */
    public Set<FailureType> getIgnoredFailureTypes() {
        return ignoredFailureTypes;
    }

    /** {@inheritDoc} */
    @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
        return !ignoredFailureTypes.contains(failureCtx.type()) && handle(ignite, failureCtx);
    }

    /**
     * Returns {@link ShutdownPolicy} to be used when handling critical situation by FailureProcessor mechanism.
     * IMMEDIATE shutdown policy is used as by default we treat any situation reached Failure Handler
     * as requiering immediate shutdown.
     */
    protected ShutdownPolicy shutdownPolicyToHandleFailure() {
        return ShutdownPolicy.IMMEDIATE;
    }

    /**
     * Actual failure handling. This method is not called for ignored failure types.
     *
     * @see #setIgnoredFailureTypes(Set).
     * @see FailureHandler#onFailure(Ignite, FailureContext).
     */
    protected abstract boolean handle(Ignite ignite, FailureContext failureCtx);

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractFailureHandler.class, this);
    }
}
