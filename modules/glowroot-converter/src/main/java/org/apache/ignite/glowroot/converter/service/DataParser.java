/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.glowroot.converter.service;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.apache.ignite.glowroot.converter.model.CacheConfigMeta;
import org.apache.ignite.glowroot.converter.model.CacheQueryTraceItem;
import org.apache.ignite.glowroot.converter.model.CacheTraceItem;
import org.apache.ignite.glowroot.converter.model.CommitTraceItem;
import org.apache.ignite.glowroot.converter.model.ComputeTraceItem;
import org.apache.ignite.glowroot.converter.model.GlowrootTransactionMeta;
import org.apache.ignite.glowroot.converter.model.TraceItem;

/**
 * Class that parses glowroot trace message strings and returns corresponing {@code TraceItem} model objects.
 */
class DataParser {
    /** **/
    private static final Logger logger = Logger.getLogger(DataParser.class.getName());

    /**
     * Pattern for parsing cache ops trace message string:
     * 'trace_type=cache_ops cache_name=CacheQueryExampleOrganizations op=put args=Long,Organization'
     */
    private static final Pattern CACHE_OPS_PATTERN = Pattern.compile("cache_name=| op=| args=");

    /**
     * Pattern for parsing cache query trace message string:
     * 'trace_type=cache_ops cache_name=CacheQueryExampleOrganizations query=Select 1;'
     */
    private static final Pattern CACHE_QUERY_PATTERN = Pattern.compile("cache_name=| query=");

    /**
     * Pattern for parsing compute trace message string.
     */
    private static final Pattern COMPUTE_PATTERN = Pattern.compile("task=");

    /**
     * Pattern for parsing ignite transaction commit trace message string.
     */
    private static final Pattern COMMIT_TX_PATTERN = Pattern.compile("label=");

    /**
     * Pattern for parsing ignite cache config trace message string.
     */
    private static final Pattern CACHE_CONFIG_PATTERN = Pattern.compile("cache_name=| config=");

    /**
     * Parses trace item message string and returns corresponfing trace item model object.
     *
     * @param txMeta Glowroot transaction metadata.
     * @param durationNanos Trace duration in nanoseconds.
     * @param offsetNanos Trace offset in nanoseconds from the beginning of transaction.
     * @param traceMsg Trace messae string.
     * @return Trace item model object.
     */
    public static TraceItem parse(GlowrootTransactionMeta txMeta, long durationNanos, long offsetNanos,
        String traceMsg) {
        if (traceMsg.startsWith("trace_type=cache_query")) {
            String[] traceAttrs = CACHE_QUERY_PATTERN.split(traceMsg);

            return new CacheQueryTraceItem(txMeta, durationNanos, offsetNanos, traceAttrs[1], traceAttrs[2]);
        }
        else if (traceMsg.startsWith("trace_type=cache_ops")) {
            String[] traceAttrs = CACHE_OPS_PATTERN.split(traceMsg);

            return new CacheTraceItem(txMeta, durationNanos, offsetNanos, traceAttrs[1], traceAttrs[2],
                traceAttrs[3]);
        }
        else if (traceMsg.startsWith("trace_type=compute")) {
            String[] traceAttrs = COMPUTE_PATTERN.split(traceMsg);

            return new ComputeTraceItem(txMeta, durationNanos, offsetNanos, traceAttrs[1]);
        }
        else if (traceMsg.startsWith("trace_type=commit_tx")) {
            String[] traceAttrs = COMMIT_TX_PATTERN.split(traceMsg);

            return new CommitTraceItem(txMeta, durationNanos, offsetNanos, traceAttrs[1]);
        }
        else {
            logger.log(Level.WARNING, "Unexpected trace message: " + traceMsg);

            return null;
        }
    }

    /**
     * Parses cache configurations messages.
     *
     * @param cacheCfgMsg Cache configurations messages.
     * @return Cache Configuration.
     */
    public static CacheConfigMeta parseCacheConfigMeta(String cacheCfgMsg) {
        assert  cacheCfgMsg.startsWith("trace_type=cache_config");

        String[] traceAttrs = CACHE_CONFIG_PATTERN.split(cacheCfgMsg);

        return new CacheConfigMeta(traceAttrs[1], traceAttrs[2]);
    }
}
