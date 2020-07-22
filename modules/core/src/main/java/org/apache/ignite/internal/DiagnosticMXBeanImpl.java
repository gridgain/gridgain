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
package org.apache.ignite.internal;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.DiagnosticMXBean;

/**
 * DiagnosticMXBean implementation.
 */
public class DiagnosticMXBeanImpl implements DiagnosticMXBean {
    /** */
    private final GridKernalContextImpl ctx;

    /**
     * @param ctx Context.
     */
    public DiagnosticMXBeanImpl(GridKernalContextImpl ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public boolean getDiagnosticMessageStatsEnabled() {
        return ctx.metric().distributedMetricsConfiguration().diagnosticMessageStatsEnabled();
    }

    /** {@inheritDoc} */
    @Override public void setDiagnosticMessageStatsEnabled(boolean enabled) {
        ctx.metric().distributedMetricsConfiguration().diagnosticMessageStatsEnabled(enabled);
    }

    /** {@inheritDoc} */
    @Override public long getDiagnosticMessageStatTooLongProcessing() {
        return ctx.metric().distributedMetricsConfiguration().diagnosticMessageStatTooLongProcessing();
    }

    /** {@inheritDoc} */
    @Override public void setDiagnosticMessageStatTooLongProcessing(long val) {
        ctx.metric().distributedMetricsConfiguration().diagnosticMessageStatTooLongProcessing(val);
    }

    /** {@inheritDoc} */
    @Override public long getDiagnosticMessageStatTooLongWaiting() {
        return ctx.metric().distributedMetricsConfiguration().diagnosticMessageStatTooLongWaiting();
    }

    /** {@inheritDoc} */
    @Override public void setDiagnosticMessageStatTooLongWaiting(long val) {
        ctx.metric().distributedMetricsConfiguration().diagnosticMessageStatTooLongWaiting(val);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiagnosticMXBeanImpl.class, this);
    }
}
