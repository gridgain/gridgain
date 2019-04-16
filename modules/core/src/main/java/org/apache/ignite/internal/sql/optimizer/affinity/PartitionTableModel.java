/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.sql.optimizer.affinity;

import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Partition join model. Describes how tables are joined with each other.
 */
public class PartitionTableModel {
    /** Join group which could not be applied (e.g. for "ALL" case). */
    public static final int GRP_NONE = -1;

    /** All tables observed during parsing excluding outer. */
    private final Map<String, PartitionTable> tbls = new HashMap<>();

    /** Join groups. */
    private final Map<Integer, PartitionJoinGroup> grps = new HashMap<>();

    /** Talbes which are excluded from partition pruning calculation. */
    private Set<String> excludedTblNames;

    /** Group index generator */
    private int grpIdxGen;

    /**
     * Add table.
     *
     * @param tbl Table.
     * @param aff Affinity descriptor.
     */
    public void addTable(PartitionTable tbl, PartitionTableAffinityDescriptor aff) {
        int grpIdx = grpIdxGen++;

        tbl.joinGroup(grpIdx);

        tbls.put(tbl.alias(), tbl);
        grps.put(grpIdx, new PartitionJoinGroup(aff).addTable(tbl));
    }

    /**
     * Get table by alias.
     *
     * @param alias Alias.
     * @return Table or {@code null} if it cannot be used for partition pruning.
     */
    @Nullable public PartitionTable table(String alias) {
        PartitionTable res = tbls.get(alias);

        assert res != null || (excludedTblNames != null && excludedTblNames.contains(alias));

        return res;
    }

    /**
     * Add excluded table
     *
     * @param alias Alias.
     */
    public void addExcludedTable(String alias) {
        PartitionTable tbl = tbls.remove(alias);

        if (tbl != null) {
            PartitionJoinGroup grp = grps.get(tbl.joinGroup());

            assert grp != null;

            if (grp.removeTable(tbl))
                grps.remove(tbl.joinGroup());
        }

        if (excludedTblNames == null)
            excludedTblNames = new HashSet<>();

        excludedTblNames.add(alias);
    }

    /**
     * Add equi-join condition. Two joined tables may possibly be merged into a single group.
     *
     * @param cond Condition.
     */
    public void addJoin(PartitionJoinCondition cond) {
        PartitionTable leftTbl = tbls.get(cond.leftAlias());
        PartitionTable rightTbl = tbls.get(cond.rightAlias());

        assert leftTbl != null || (excludedTblNames != null && excludedTblNames.contains(cond.leftAlias()));
        assert rightTbl != null || (excludedTblNames != null && excludedTblNames.contains(cond.rightAlias()));

        // At least one tables is excluded, return.
        if (leftTbl == null || rightTbl == null)
            return;

        // At least one column in condition is not affinity column, return.
        if (!leftTbl.isAffinityColumn(cond.leftColumn()) || !rightTbl.isAffinityColumn(cond.rightColumn()))
            return;

        // Remember join group of the right table as it will be changed below.
        int rightGrpId = rightTbl.joinGroup();

        PartitionJoinGroup leftGrp = grps.get(leftTbl.joinGroup());
        PartitionJoinGroup rightGrp = grps.get(rightGrpId);

        assert leftGrp != null;
        assert rightGrp != null;

        // Groups are not compatible, return.
        if (!leftGrp.affinityDescriptor().isCompatible(rightGrp.affinityDescriptor()))
            return;

        // Safe to merge groups.
        for (PartitionTable tbl : rightGrp.tables()) {
            tbl.joinGroup(leftTbl.joinGroup());

            leftGrp.addTable(tbl);
        }

        grps.remove(rightGrpId);
    }

    /**
     * Get affinity descriptor for the group.
     *
     * @param grpId Group ID.
     * @return Affinity descriptor or {@code null} if there is no affinity descriptor (e.g. for "NONE" result).
     */
    @Nullable public PartitionTableAffinityDescriptor joinGroupAffinity(int grpId) {
        if (grpId == GRP_NONE)
            return null;

        PartitionJoinGroup grp = grps.get(grpId);

        assert grp != null;

        return grp.affinityDescriptor();
    }
}
