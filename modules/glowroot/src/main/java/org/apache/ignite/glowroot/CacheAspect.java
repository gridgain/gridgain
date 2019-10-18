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

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.cache.configuration.Configuration;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.glowroot.agent.plugin.api.Agent;
import org.glowroot.agent.plugin.api.MessageSupplier;
import org.glowroot.agent.plugin.api.OptionalThreadContext;
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
import org.glowroot.agent.plugin.api.weaving.Shim;

/**
 * Trace cache operations.
 */
public class CacheAspect {

    /** Cache configuration map, in order to log every new cache configuration ones only. **/
    static ConcurrentHashMap<String, String> cacheConfigurations = new ConcurrentHashMap<>();

    /** Stub for valus in cacheConfigurations, cause we really need only key. **/
    private static final String EMPTY_STRING = "";

    /**
     * Per thread tx context holder.
     */
    private static ThreadLocal<TraceEntry> txTraceCtx = new ThreadLocal<>();

    /** */
    @Shim("org.apache.ignite.IgniteCache")
    public interface IgniteCache {
        /** */
        String getName();
    }

    /** */
    @Shim("org.apache.ignite.internal.binary.BinaryObjectEx")
    public interface BinaryObjectEx {
        /** */
        int typeId();
    }

    /** */
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
        public static TraceEntry onBefore(OptionalThreadContext ctx, @BindReceiver IgniteCache proxy,
            @BindMethodName String val, @BindParameterArray Object[] params) {
            // TODO: 15.10.19 Use some cache unique identifier, like cache creation time, or uuid. deplumentId
            // Used in order to trace cache configuration in separate glowroot transaction.
            if (cacheConfigurations.putIfAbsent(proxy.getName(), EMPTY_STRING) == null) {
                try {
                    Method getConfigurationMethod = proxy.getClass().getMethod("getConfiguration", Class.class);

                    Object res = getConfigurationMethod.invoke(proxy, Configuration.class);

                    ctx.startTransaction("Ignite Cache Meta",
                        Thread.currentThread().getName(),
                        MessageSupplier.create("trace_type=cache_config cache_name={}, config={}",
                            proxy.getName(),
                            res.toString()),
                        timer,
                        OptionalThreadContext.AlreadyInTransactionBehavior.CAPTURE_NEW_TRANSACTION).end();
                }
                catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

            // Used in order to trace non-transactional Ignite cache operations.
            if (!ctx.isInTransaction()) {
                TraceEntry txTraceEntry = ctx.startTransaction("Ignite",
                    Thread.currentThread().getName(),
                    MessageSupplier.create(""),
                    timer);

                txTraceCtx.set(txTraceEntry);
            }

            if ("query".equals(val))
                return ctx.startTraceEntry(
                    MessageSupplier.create("trace_type=cache_query cache_name={} query={}",
                        proxy.getName(),
                        params[0].toString()),
                    timer);
            else {
                String args = Arrays.stream(params).map(param -> ReflectionToStringBuilder.reflectionToString(param)).collect(Collectors.joining(","));

                return ctx.startTraceEntry(
                    MessageSupplier.create("trace_type=cache_ops cache_name={} op={} args={}",
                        proxy.getName(),
                        val,
                        args),
                    timer);
            }
        }

        /**
         * @param traceEntry Trace entry.
         */
        @OnReturn
        public static void onReturn(@BindTraveler TraceEntry traceEntry) {
            traceEntry.end();

            TraceEntry txTraceEntry = txTraceCtx.get();

            if (txTraceEntry != null)
                txTraceEntry.end();
        }

        /**
         * @param throwable Throwable.
         * @param traceEntry Trace entry.
         */
        @OnThrow
        public static void onThrow(@BindThrowable Throwable throwable,
            @BindTraveler TraceEntry traceEntry) {
            traceEntry.endWithError(throwable);

            TraceEntry txTraceEntry = txTraceCtx.get();

            if (txTraceEntry != null)
                txTraceEntry.endWithError(throwable);
        }
    }
}