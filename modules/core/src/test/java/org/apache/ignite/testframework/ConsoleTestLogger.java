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

package org.apache.ignite.testframework;

import org.apache.ignite.IgniteLogger;

/**
 * Console logger for tests.
 */
public class ConsoleTestLogger implements IgniteLogger {
    /**
     *
     */
    private final String logOwner;

    /**
     *
     */
    public ConsoleTestLogger(String logOwner) {
        this.logOwner = logOwner;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return new ConsoleTestLogger(ctgr.getClass().getName());
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        System.out.println(logOwner + " " + msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        System.out.println(logOwner + " " + msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        System.out.println(logOwner + " " + msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, Throwable e) {
        System.err.println(logOwner + " " + msg);

        if (e != null)
            e.printStackTrace();
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, Throwable e) {
        System.err.println(logOwner + " " + msg);

        if (e != null)
            e.printStackTrace();
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String fileName() {
        return null;
    }
}
