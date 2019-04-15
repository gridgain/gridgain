/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.lang.IgniteCallable;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * Closure that is computed on near node to get the stack trace of active transaction owner thread.
 */
public class FetchActiveTxOwnerTraceClosure implements IgniteCallable<String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final StackTraceElement[] STACK_TRACE_ELEMENT_EMPTY = new StackTraceElement[0];

    /** */
    private final long txOwnerThreadId;

    /** */
    public FetchActiveTxOwnerTraceClosure(long txOwnerThreadId) {
        this.txOwnerThreadId = txOwnerThreadId;
    }

    /**
     * Builds the stack trace dump of the transaction owner thread
     *
     * @return stack trace dump string
     * @throws Exception If failed
     */
    @Override public String call() throws Exception {
        StringBuilder traceDump = new StringBuilder("Stack trace of the transaction owner thread:\n");

        for (StackTraceElement stackTraceElement : getStackTrace()) {
            traceDump.append(stackTraceElement.toString());
            traceDump.append("\n");
        }

        return traceDump.toString();
    }

    /**
     * Gets the stack trace of the transaction owner thread
     *
     * @return stack trace elements
     */
    private StackTraceElement[] getStackTrace() {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        ThreadInfo threadInfo;

        try {
            threadInfo = threadMXBean.getThreadInfo(txOwnerThreadId, Integer.MAX_VALUE);
        }
        catch (SecurityException | IllegalArgumentException ignored) {
            threadInfo = null;
        }

        return threadInfo == null ? STACK_TRACE_ELEMENT_EMPTY : threadInfo.getStackTrace();
    }
}
