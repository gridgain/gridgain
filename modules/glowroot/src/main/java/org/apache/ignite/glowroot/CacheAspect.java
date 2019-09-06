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

import java.util.Collection;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl;
import org.apache.ignite.transactions.TransactionException;
import org.glowroot.agent.plugin.api.Agent;
import org.glowroot.agent.plugin.api.AuxThreadContext;
import org.glowroot.agent.plugin.api.Logger;
import org.glowroot.agent.plugin.api.MessageSupplier;
import org.glowroot.agent.plugin.api.ParameterHolder;
import org.glowroot.agent.plugin.api.ThreadContext;
import org.glowroot.agent.plugin.api.Timer;
import org.glowroot.agent.plugin.api.TimerName;
import org.glowroot.agent.plugin.api.TraceEntry;
import org.glowroot.agent.plugin.api.checker.Nullable;
import org.glowroot.agent.plugin.api.weaving.BindClassMeta;
import org.glowroot.agent.plugin.api.weaving.BindParameter;
import org.glowroot.agent.plugin.api.weaving.BindReceiver;
import org.glowroot.agent.plugin.api.weaving.BindThrowable;
import org.glowroot.agent.plugin.api.weaving.BindTraveler;
import org.glowroot.agent.plugin.api.weaving.IsEnabled;
import org.glowroot.agent.plugin.api.weaving.Mixin;
import org.glowroot.agent.plugin.api.weaving.OnAfter;
import org.glowroot.agent.plugin.api.weaving.OnBefore;
import org.glowroot.agent.plugin.api.weaving.OnReturn;
import org.glowroot.agent.plugin.api.weaving.OnThrow;
import org.glowroot.agent.plugin.api.weaving.Pointcut;
import org.glowroot.agent.plugin.api.weaving.Shim;

public class CacheAspect {
    // Glowroot will inject this interface into com.example.myapp.Invoice's interface list
    // so that the plugin can access it regardless of what class loader loads com.example.myapp.Invoice
//    @Shim("org.apache.ignite.IgniteCache")
//    public interface IgniteCache<K, V> extends javax.cache.Cache<K, V> {
//        public void put(K key, V val) throws TransactionException;
//    }

    /**
     */
    @Pointcut(className = "org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl",
        methodName = "put",
        methodParameterTypes = {"java.lang.Object", "java.lang.Object"},
        timerName = "cache_put"
    )
    public static class CachePutAdvice {
        private static final TimerName timer = Agent.getTimerName(CachePutAdvice.class);

        @OnBefore
        public static TraceEntry onBefore(ThreadContext context, @BindParameter Object key, @BindParameter Object value) {
            return context.startTraceEntry(
                MessageSupplier.create("cache put: {} {}", key.toString(), value.toString()), timer);
        }

        @OnReturn
        public static void onReturn(@BindTraveler TraceEntry traceEntry) {
            traceEntry.end();
        }

        @OnThrow
        public static void onThrow(@BindThrowable Throwable throwable,
            @BindTraveler TraceEntry traceEntry) {
            traceEntry.endWithError(throwable);
        }
    }
}