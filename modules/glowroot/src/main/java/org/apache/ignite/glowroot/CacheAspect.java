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
package org.apache.ignite.glowroot;

import org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.typedef.X;
import org.glowroot.agent.plugin.api.Agent;
import org.glowroot.agent.plugin.api.MessageSupplier;
import org.glowroot.agent.plugin.api.ThreadContext;
import org.glowroot.agent.plugin.api.TimerName;
import org.glowroot.agent.plugin.api.TraceEntry;
import org.glowroot.agent.plugin.api.weaving.BindMethodName;
import org.glowroot.agent.plugin.api.weaving.BindParameterArray;
import org.glowroot.agent.plugin.api.weaving.BindReceiver;
import org.glowroot.agent.plugin.api.weaving.BindThrowable;
import org.glowroot.agent.plugin.api.weaving.BindTraveler;
import org.glowroot.agent.plugin.api.weaving.OnBefore;
import org.glowroot.agent.plugin.api.weaving.OnReturn;
import org.glowroot.agent.plugin.api.weaving.OnThrow;
import org.glowroot.agent.plugin.api.weaving.Pointcut;

/**
 * Trace cache operations.
 */
public class CacheAspect {
    /**
     */
    @Pointcut(className = "org.apache.ignite.IgniteCache",
        subTypeRestriction = "org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl",
        methodName = "*",
        methodParameterTypes = {".."},
        timerName = "cache_op",
        suppressionKey = "cache",
        suppressibleUsingKey = "cache"
    )
    public static class CachePutAdvice {
        /** */
        private static final TimerName timer = Agent.getTimerName(CachePutAdvice.class);

        /**
         * @param ctx Context.
         * @param val Value.
         */
        @OnBefore
        public static TraceEntry onBefore(ThreadContext ctx, @BindReceiver IgniteCacheProxyImpl proxy, @BindMethodName String val, @BindParameterArray Object[] params) {
            return "query".equals(val) ?
                ctx.startTraceEntry(MessageSupplier.create("cache name={} query={}", proxy.getName(), params[0].toString()), timer) :
                ctx.startTraceEntry(MessageSupplier.create("cache name={} op={}", proxy.getName(), val), timer);
        }

        /**
         * @param traceEntry Trace entry.
         */
        @OnReturn
        public static void onReturn(@BindTraveler TraceEntry traceEntry) {
            traceEntry.end();
        }

        /**
         * @param throwable Throwable.
         * @param traceEntry Trace entry.
         */
        @OnThrow
        public static void onThrow(@BindThrowable Throwable throwable,
            @BindTraveler TraceEntry traceEntry) {
            traceEntry.endWithError(throwable);
        }
    }
}