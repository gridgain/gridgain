package org.apache.ignite.internal.visor.ru;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.ru.RollingUpgradeStatus;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Gets cluster-wide state of rolling upgrade.
 */
@GridInternal
@GridVisorManagementTask
public class VisorRollingUpgradeStatusTask extends VisorOneNodeTask<Void, RollingUpgradeStatus> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Void, RollingUpgradeStatus> job(Void arg) {
        return new VisorRollingUpgradeStatusJob(arg, debug);
    }

    /**
     * Job that actually gets the status of rolling upgrade.
     */
    private static class VisorRollingUpgradeStatusJob extends VisorJob<Void, RollingUpgradeStatus> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Creates the job.
         *
         * @param info Snapshot info.
         * @param debug Debug flag.
         */
        VisorRollingUpgradeStatusJob(Void info, boolean debug) {
            super(info, debug);
        }

        /** {@inheritDoc} */
        @Override protected RollingUpgradeStatus run(Void arg) throws IgniteException {
            return ignite.context().rollingUpgrade().getRollingUpgradeStatus();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorRollingUpgradeStatusJob.class, this);
        }
    }
}
