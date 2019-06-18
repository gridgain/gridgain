package org.apache.ignite.internal.visor.cache.affinityView;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Argument for {@link VisorAffinityViewTask}
 */
public class VisorAffinityViewTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    public enum Mode {
        /** Ideal. */
        IDEAL(AffinityViewCommandArg.IDEAL),

        /** Current. */
        CURRENT(AffinityViewCommandArg.CURRENT),

        /** Diff. */
        DIFF(AffinityViewCommandArg.DIFF);

        /** Option name. */
        private final AffinityViewCommandArg modeName;

        /**
         * @param modeName arg modeName
         */
        Mode(AffinityViewCommandArg modeName) {
            this.modeName = modeName;
        }

        /**
         * @param avArg AffinityViewCommandArg.
         * @return {@link Mode} value corresponding to avArg.
         */
        public static Mode fromAVCmdArg(AffinityViewCommandArg avArg) {
            for (Mode m: values()) {
                if (m.modeName == avArg)
                    return m;
            }

            return null;
        }
    }

    /** */
    private String cacheGrpName;
    /** */
    private Mode mode;

    /**
     * Default constructor. Required for {@link Externalizable} support.
     */
    public VisorAffinityViewTaskArg() {
        // No-op.
    }

    /**
     * @param cacheGrpName Group name.
     * @param mode Mode.
     */
    public VisorAffinityViewTaskArg(String cacheGrpName, Mode mode) {
        this.cacheGrpName = cacheGrpName;
        this.mode = mode;

    }

    /** {@inheritDoc} */
    @Override
    protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cacheGrpName);
        U.writeEnum(out, mode);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheGrpName = U.readString(in);
        mode = U.readEnum(in, Mode.class);
    }

    /**
     * @return {@code cacheGrpName}
     */
    public String getCacheGrpName() {
        return cacheGrpName;
    }

    /**
     * @return {@code mode}
     */
    public Mode getMode() {
        return mode;
    }
}
