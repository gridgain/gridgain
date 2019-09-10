package org.apache.ignite.glowroot;

import org.glowroot.agent.plugin.api.Agent;
import org.glowroot.agent.plugin.api.MessageSupplier;
import org.glowroot.agent.plugin.api.ThreadContext;
import org.glowroot.agent.plugin.api.TimerName;
import org.glowroot.agent.plugin.api.TraceEntry;
import org.glowroot.agent.plugin.api.weaving.BindMethodName;
import org.glowroot.agent.plugin.api.weaving.BindThrowable;
import org.glowroot.agent.plugin.api.weaving.BindTraveler;
import org.glowroot.agent.plugin.api.weaving.OnBefore;
import org.glowroot.agent.plugin.api.weaving.OnReturn;
import org.glowroot.agent.plugin.api.weaving.OnThrow;
import org.glowroot.agent.plugin.api.weaving.Pointcut;

/**
 * Trace closure and task execution.
 */
public class ComputeAspect {
    /**
     */
    @Pointcut(className = "org.apache.ignite.internal.processors.task.GridTaskProcessor",
        methodName = "execute",
        methodParameterTypes = {".."},
        timerName = "task_execute"
    )
    public static class CachePutAdvice {
        private static final TimerName timer = Agent.getTimerName(CachePutAdvice.class);

        @OnBefore
        public static TraceEntry onBefore(ThreadContext context, @BindMethodName String val) {
            return context.startTraceEntry(MessageSupplier.create("cache {}", val), timer);
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
