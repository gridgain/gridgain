package org.apache.ignite.glowroot;

import org.glowroot.agent.plugin.api.Agent;
import org.glowroot.agent.plugin.api.MessageSupplier;
import org.glowroot.agent.plugin.api.OptionalThreadContext;
import org.glowroot.agent.plugin.api.TimerName;
import org.glowroot.agent.plugin.api.TraceEntry;
import org.glowroot.agent.plugin.api.weaving.BindParameterArray;
import org.glowroot.agent.plugin.api.weaving.BindReceiver;
import org.glowroot.agent.plugin.api.weaving.BindReturn;
import org.glowroot.agent.plugin.api.weaving.BindThrowable;
import org.glowroot.agent.plugin.api.weaving.BindTraveler;
import org.glowroot.agent.plugin.api.weaving.OnAfter;
import org.glowroot.agent.plugin.api.weaving.OnBefore;
import org.glowroot.agent.plugin.api.weaving.OnReturn;
import org.glowroot.agent.plugin.api.weaving.OnThrow;
import org.glowroot.agent.plugin.api.weaving.Pointcut;
import org.glowroot.agent.plugin.api.weaving.Shim;

/**
 */
public class TransactionAspect {
    /** */
    private static final int SAMPLE_RATE = 1_000;

    /** */
    private static final String TRACE_LOG = "Trace";

    /**
     * Per thread tx context holder.
     */
    private static ThreadLocal<TraceEntry> traceCtx = new ThreadLocal<>();

    /**
     * Per thread tx counter for sampling.
     */
    private static ThreadLocal<long[]> samplingCtx = new ThreadLocal<long[]>() {
        @Override protected long[] initialValue() {
            return new long[1];
        }
    };

    /** */
    @Shim("org.apache.ignite.transactions.Transaction")
    public interface Transaction {
        /** Label. */
        String label();
    }

    /** */
    @Pointcut(className = "org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager",
        methodName = "newTx",
        nestingGroup = "ignite",
        methodParameterTypes = {".."},
        timerName = "process_tx")
    public static class TxStartAdvice {
        /** Timer. */
        private static final TimerName timer = Agent.getTimerName(TxStartAdvice.class);

        /**
         * @param ctx Context.
         * @param params Params.
         */
        @OnBefore
        public static TraceEntry onBefore(OptionalThreadContext ctx,
            @BindReceiver Object mgr,
            @BindParameterArray Object[] params) {
            return ctx.startTransaction("Ignite",
                Thread.currentThread().getName(),
                MessageSupplier.create(""),
                timer);
        }

        /**
         * @param ret Ret.
         * @param traceEntry Trace entry.
         */
        @OnReturn
        public static void onReturn(@BindReturn Object ret, @BindTraveler TraceEntry traceEntry) {
            traceCtx.set(traceEntry);
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

    /** */
    @Pointcut(className = "org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl",
        methodName = "commit|rollback",
        methodParameterTypes = {},
        timerName = "finish_tx")
    public static class TxFinishAdvice {
        /** */
        private static final TimerName timer = Agent.getTimerName(TxFinishAdvice.class);

        /**
         * @param ctx Context.
         */
        @OnBefore
        public static TraceEntry onBefore(OptionalThreadContext ctx, @BindReceiver Transaction proxy) {
            return ctx.startTraceEntry(MessageSupplier.create("commit tx: label={}", proxy.label()),
                timer);
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

        @OnAfter
        public static void onAfter() {
            TraceEntry entry = traceCtx.get();

            if (entry != null) {
                long cntr = samplingCtx.get()[0];
                samplingCtx.get()[0] = cntr + 1;

                if (cntr % SAMPLE_RATE == 0)
                    entry.endWithError(TRACE_LOG);
                else
                    entry.end();

                traceCtx.set(null);
            }
        }
    }
}
