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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;

/**
 * Long running query manager.
 */
public final class LongRunningQueryManager {
    /** Check period in ms. */
    private static final long CHECK_PERIOD = 1_000;

    /** Connection manager. */
    private final ConnectionManager connMgr;

    /** Queries collection. Sorted collection isn't used to reduce 'put' time. */
    private final ConcurrentHashMap<H2QueryInfo, TimeoutChecker> qrys = new ConcurrentHashMap<>();

    /** Check worker. */
    private final GridWorker checkWorker;

    /** Logger. */
    private final IgniteLogger log;

    /** Long query timeout milliseconds. */
    private volatile long timeout;

    /**
     * Long query timeout multiplier. The warning will be printed after:
     * - timeout;
     * - timeout * multiplier;
     * - timeout * multiplier * multiplier;
     * - etc...
     *
     * If the multiplier <= 1, the warning message is printed once.
     */
    private volatile int timeoutMult = 2;

    /**
     * @param ctx Kernal context.
     */
    public LongRunningQueryManager(GridKernalContext ctx) {
        connMgr = ((IgniteH2Indexing)ctx.query().getIndexing()).connections();

        log = ctx.log(LongRunningQueryManager.class);

        checkWorker = new GridWorker(ctx.igniteInstanceName(), "long-qry", log) {
            @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
                while (true) {
                    checkLongRunning();

                    U.sleep(CHECK_PERIOD);
                }
            }
        };

        timeout = ctx.config().getLongQueryWarningTimeout();

        Thread thread = new Thread(checkWorker);

        thread.setDaemon(true);
        thread.start();
    }

    /**
     *
     */
    public void stop() {
        checkWorker.cancel();

        qrys.clear();
    }

    /**
     * @param qryInfo Query info to register.
     */
    public void registerQuery(H2QueryInfo qryInfo) {
        assert qryInfo != null;

        final long timeout0 = timeout;

        if (timeout0 > 0)
            qrys.put(qryInfo, new TimeoutChecker(timeout0, timeoutMult));
    }

    /**
     * @param qryInfo Query info to remove.
     */
    public void unregisterQuery(H2QueryInfo qryInfo) {
        assert qryInfo != null;

        qrys.remove(qryInfo);
    }

    /**
     *
     */
    private void checkLongRunning() {
        for (Map.Entry<H2QueryInfo, TimeoutChecker> e : qrys.entrySet()) {
            H2QueryInfo qinfo = e.getKey();

            if (e.getValue().checkTimeout(qinfo.time())) {
                qinfo.printLogMessage(log, connMgr, "Query execution is too long");

                if (e.getValue().timeoutMult <= 1)
                    qrys.remove(qinfo);
            }
        }
    }

    /**
     * @return Timeout in milliseconds after which long query warning will be printed.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Sets timeout in milliseconds after which long query warning will be printed.
     *
     * @param timeout Timeout in milliseconds after which long query warning will be printed.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Long query timeout multiplier.
     */
    public int getTimeoutMultiplier() {
        return timeoutMult;
    }

    /**
     * Sets long query timeout multiplier. The warning will be printed after:
     *      - timeout;
     *      - timeout * multiplier;
     *      - timeout * multiplier * multiplier;
     *      - etc...
     * If the multiplier <= 1, the warning message is printed once.
     *
     * @param timeoutMult Long query timeout multiplier.
     */
    public void setTimeoutMultiplier(int timeoutMult) {
        this.timeoutMult = timeoutMult;
    }

    /**
     * Holds timeout settings for the specified query.
     */
    private static class TimeoutChecker {
        /** */
        private long timeout;

        /** */
        private int timeoutMult;

        /**
         * @param timeout Initial timeout.
         * @param timeoutMult Timeout multiplier.
         */
        public TimeoutChecker(long timeout, int timeoutMult) {
            this.timeout = timeout;
            this.timeoutMult = timeoutMult;
        }

        /**
         * @param time Query execution time.
         * @return {@code true} if timeout occurred.
         */
        public boolean checkTimeout(long time) {
            if (time > timeout) {
                if (timeoutMult > 1)
                    timeout *= timeoutMult;

                return true;
            }
            else
                return false;
        }
    }
}
