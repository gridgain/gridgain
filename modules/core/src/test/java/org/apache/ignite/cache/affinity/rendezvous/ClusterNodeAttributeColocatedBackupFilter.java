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

    @Override
    public boolean apply(ClusterNode candidate, List<ClusterNode> previouslySelected) {
        Iterator var3 = previouslySelected.iterator();
        if (var3.hasNext()) {
            ClusterNode node = (ClusterNode)var3.next();
            return Objects.equals(candidate.attribute(this.attributeName), node.attribute(this.attributeName));
        } else {
            return true;
        }
    }
}