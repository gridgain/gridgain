package org.apache.ignite.internal.visor.ru;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.ru.RollingUpgradeModeChangeResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * The task that represents enabling/disabling rolling upgrade mode.
 */
@GridInternal
@GridVisorManagementTask
public class VisorRollingUpgradeChangeModeTask extends VisorOneNodeTask<VisorRollingUpgradeChangeModeTaskArg, RollingUpgradeModeChangeResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorRollingUpgradeJob job(VisorRollingUpgradeChangeModeTaskArg arg) {
        return new VisorRollingUpgradeJob(arg, debug);
    }

    /**
     * Job that will collect baseline topology information.
     */
    private static class VisorRollingUpgradeJob extends VisorJob<VisorRollingUpgradeChangeModeTaskArg, RollingUpgradeModeChangeResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorRollingUpgradeJob(VisorRollingUpgradeChangeModeTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected RollingUpgradeModeChangeResult run(
            @Nullable VisorRollingUpgradeChangeModeTaskArg arg
        ) throws IgniteException {
            RollingUpgradeModeChangeResult res;

            switch (arg.operation()) {
                case ENABLE:
                    res = ignite.context().rollingUpgrade().setRollingUpgradeMode(true);

                    if (arg.isForcedMode())
                        ignite.context().rollingUpgrade().enableForcedRollingUpgradeMode();

                    break;

                case DISABLE:
                    res = ignite.context().rollingUpgrade().setRollingUpgradeMode(false);

                    break;

                default:
                    throw new IgniteException("Unexpected rolling upgrade operation arg=[" + arg + ']');
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorRollingUpgradeJob.class, this);
        }
    }
}
