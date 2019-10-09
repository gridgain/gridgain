package org.apache.ignite.glowroot;

import org.glowroot.agent.plugin.api.Agent;
import org.glowroot.agent.plugin.api.MessageSupplier;
import org.glowroot.agent.plugin.api.ThreadContext;
import org.glowroot.agent.plugin.api.TimerName;
import org.glowroot.agent.plugin.api.TraceEntry;
import org.glowroot.agent.plugin.api.weaving.BindMethodName;
import org.glowroot.agent.plugin.api.weaving.BindParameterArray;
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
    /** */
    @Pointcut(className = "org.apache.ignite.internal.processors.task.GridTaskProcessor",
        methodName = "execute",
        methodParameterTypes = {".."},
        timerName = "task_execute",
        suppressibleUsingKey = "task",
        suppressionKey = "task"
    )
    public static class TaskAdvice {
        /** */
        private static final TimerName timer = Agent.getTimerName(TaskAdvice.class);

        /**
         * @param ctx Context.
         * @param val Value.
         * @param params Params.
         */
        @OnBefore
        public static TraceEntry onBefore(ThreadContext ctx, @BindMethodName String val, @BindParameterArray Object[] params) {
            StringBuilder b = new StringBuilder(500);
            for (Object param : params) {
                b.append(param == null ? "NULL" : param.toString());
                b.append(" ");
            }

            return ctx.startTraceEntry(MessageSupplier.create("trace_type=compute task={}", b.toString()), timer);
        }

        /** */
        @OnReturn
        public static void onReturn(@BindTraveler TraceEntry traceEntry) {
            traceEntry.end();
        }

        /** */
        @OnThrow
        public static void onThrow(@BindThrowable Throwable throwable,
            @BindTraveler TraceEntry traceEntry) {
            traceEntry.endWithError(throwable);
        }
    }
}
