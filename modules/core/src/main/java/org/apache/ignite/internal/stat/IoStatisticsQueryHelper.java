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

package org.apache.ignite.internal.stat;

/**
 * Helper for gathering IO statistics.
 */
public class IoStatisticsQueryHelper {
    /** */
    private static final ThreadLocal<IoStatisticsHolderQuery> CUR_QRY_STATS = new ThreadLocal<>();

    /**
     * Start gathering IO statistics for query. Should be used together with {@code finishGatheringQueryStatistics}
     * method.
     *
     * @param qryId Identifier of query.
     */
    public static void startGatheringQueryStatistics(String qryId) {
        IoStatisticsHolderQuery currQryStatisticsHolder = CUR_QRY_STATS.get();

        assert currQryStatisticsHolder == null : currQryStatisticsHolder;

        CUR_QRY_STATS.set(new IoStatisticsHolderQuery(qryId));
    }

    /**
     * Merge query statistics.
     *
     * @param qryStat Statistics which will be merged to current query statistics.
     */
    public static void mergeQueryStatistics(IoStatisticsHolderQuery qryStat) {
        assert qryStat != null;

        IoStatisticsHolderQuery currQryStatisticsHolder = CUR_QRY_STATS.get();

        assert currQryStatisticsHolder != null;

        currQryStatisticsHolder.merge(qryStat.logicalReads(), qryStat.physicalReads());
    }

    /**
     * Finish gathering IO statistics for query. Should be used together with {@code startGatheringQueryStatistics}
     * method.
     *
     * @return Gathered statistics.
     */
    public static IoStatisticsHolder finishGatheringQueryStatistics() {
        IoStatisticsHolderQuery currQryStatisticsHolder = CUR_QRY_STATS.get();

        assert currQryStatisticsHolder != null;

        CUR_QRY_STATS.remove();

        return currQryStatisticsHolder;
    }

    /**
     * Track logical read for current query.
     *
     * @param pageAddr Address of page.
     */
    static void trackLogicalReadQuery(long pageAddr) {
        IoStatisticsHolderQuery currQryStatisticsHolder = CUR_QRY_STATS.get();

        if (currQryStatisticsHolder != null)
            currQryStatisticsHolder.trackLogicalRead(pageAddr);

    }

    /**
     * Track physical and logical read for current query.
     *
     * @param pageAddr Address of page.
     */
    static void trackPhysicalAndLogicalReadQuery(long pageAddr) {
        IoStatisticsHolderQuery currQryStatisticsHolder = CUR_QRY_STATS.get();

        if (currQryStatisticsHolder != null)
            currQryStatisticsHolder.trackPhysicalAndLogicalRead(pageAddr);
    }

}
