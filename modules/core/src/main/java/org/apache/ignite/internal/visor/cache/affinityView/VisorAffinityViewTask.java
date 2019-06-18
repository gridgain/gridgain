package org.apache.ignite.internal.visor.cache.affinityView;

import java.util.Collections;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
//TODO: docs
@GridInternal
public class VisorAffinityViewTask extends VisorOneNodeTask<VisorAffinityViewTaskArg, AffinityViewerTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Job for fetching affinity assignment from custer
     */
    private static class VisorAffinityViewJob extends VisorJob<VisorAffinityViewTaskArg, AffinityViewerTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug flag.
         */
        VisorAffinityViewJob(VisorAffinityViewTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override
        protected AffinityViewerTaskResult run(@Nullable VisorAffinityViewTaskArg arg) throws IgniteException {
            if (arg == null)
                throw new IgniteException("VisorAffinityViewTaskArg is null");

            int groupId = CU.cacheId(arg.getCacheGrpName());

            AffinityAssignment affAss = ignite.context()
                                              .cache()
                                              .context()
                                              .affinity()
                                              .affinity(groupId)
                                              .getLatestAffinityAssignment();

            VisorAffinityViewTaskArg.Mode mode = arg.getMode();

            switch (mode) {
                case CURRENT:
                    return new AffinityViewerTaskResult(affAss.assignment(), Collections.emptySet());

                case IDEAL:
                    return new AffinityViewerTaskResult(affAss.idealAssignment(), Collections.emptySet());

                case DIFF:
                    return new AffinityViewerTaskResult(Collections.emptyList(), affAss.partitionPrimariesDifferentToIdeal());

                default:
                    throw new IgniteException("Unexpected mode: " + mode);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    protected VisorAffinityViewJob job(VisorAffinityViewTaskArg arg) {
        return new VisorAffinityViewJob(arg, debug);
    }
}
