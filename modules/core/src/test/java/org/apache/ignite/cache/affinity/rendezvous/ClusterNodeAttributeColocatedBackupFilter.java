package org.apache.ignite.cache.affinity.rendezvous;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteBiPredicate;

public class ClusterNodeAttributeColocatedBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
    private static final long serialVersionUID = 1L;
    private final String attributeName;

    public ClusterNodeAttributeColocatedBackupFilter(String attributeName) {
        this.attributeName = attributeName;
    }

    String getNodeID(ClusterNode candidate) {
        return candidate.id().toString();
    }

    @Override
    public boolean apply(ClusterNode candidate, List<ClusterNode> previouslySelected) {
        Iterator var3 = previouslySelected.iterator();

        assert candidate != null : "primary is null";

        if (var3.hasNext()) {
            ClusterNode node = (ClusterNode) var3.next();

            assert node != null : "backup is null";

            boolean filterResult = Objects.equals(candidate.attribute(this.attributeName), node.attribute(this.attributeName));
           // System.out.println("apply nasNext primary node ID : " + getNodeID(candidate) + " backup node ID : " + getNodeID(node) + " filterResult = " + filterResult);
            return filterResult;
        } else {
            System.out.println("apply not hasNext filterResult = " + true);
            return true;
        }
    }
}