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

package org.apache.ignite.logger;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Logger which does not output anything.
 */
public class NullLogger implements IgniteLogger {
    /** Singleton instance. */
    public static final NullLogger INSTANCE = new NullLogger();

    /**
     * @param log Logger.
     * @return Specified logger if it is not {@code null}, {@code NullLogger} otherwise.
     */
    public static IgniteLogger whenNull(IgniteLogger log) {
        return log == null ? INSTANCE : log;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NullLogger.class, this);
    }
}