package org.apache.ignite.internal.visor.cache.affinityView;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result object for {@link VisorAffinityViewTask}
 */
public class AffinityViewerTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;
    /** */
    private List<List<ClusterNode>> assignment;
    /** */
    private Set<Integer> primariesDifferentToIdeal;

    /**
     * Default constructor. Required for {@link Externalizable} support.
     */
    public AffinityViewerTaskResult() {
        // No-op.
    }

    /** */
    public AffinityViewerTaskResult(List<List<ClusterNode>> assignment,
        Set<Integer> primariesDifferentToIdeal)
    {
        this.assignment = assignment;
        this.primariesDifferentToIdeal = primariesDifferentToIdeal;
    }

    /** {@inheritDoc} */
    @Override
    protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, assignment);
        U.writeCollection(out, primariesDifferentToIdeal);

    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        assignment = U.readList(in);
        primariesDifferentToIdeal = U.readIntSet(in);
    }

    /**
     * @return {@code assignment}
     */
    public List<List<ClusterNode>> getAssignment() {
        return assignment;
    }

    /**
     * @return {@code primariesDifferentToIdeal}
     */
    public Set<Integer> getPrimariesDifferentToIdeal() {
        return primariesDifferentToIdeal;
    }
}
