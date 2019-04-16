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

import org.apache.ignite.internal.util.typedef.F;

/**
 * Join condition.
 */
public class PartitionJoinCondition {
    /** Cross JOIN. */
    public static final PartitionJoinCondition CROSS = new PartitionJoinCondition(null, null, null, null, true);

    /** Left alias. */
    private final String leftAlias;

    /** Right alias. */
    private final String rightAlias;

    /** Left column name. */
    private final String leftCol;

    /** Right column name. */
    private final String rightCol;

    /** Whether this is a cross-join. */
    private final boolean cross;

    /**
     * Constructor.
     *
     * @param leftAlias Left alias.
     * @param rightAlias Right alias.
     * @param leftCol Left column name.
     * @param rightCol Right column name.
     */
    public PartitionJoinCondition(String leftAlias, String rightAlias, String leftCol, String rightCol) {
        this(leftAlias, rightAlias, leftCol, rightCol, false);
    }

    /**
     * Constructor.
     *
     * @param leftAlias Left alias.
     * @param rightAlias Right alias.
     * @param leftCol Left column name.
     * @param rightCol Right column name.
     * @param cross Whether this is a cross-join.
     */
    private PartitionJoinCondition(String leftAlias, String rightAlias, String leftCol, String rightCol,
        boolean cross) {
        this.leftAlias = leftAlias;
        this.rightAlias = rightAlias;
        this.leftCol = leftCol;
        this.rightCol = rightCol;
        this.cross = cross;
    }

    /**
     * Left alias.
     */
    public String leftAlias() {
        return leftAlias;
    }

    /**
     * Right alias.
     */
    public String rightAlias() {
        return rightAlias;
    }

    /**
     * @return Left column.
     */
    public String leftColumn() {
        return leftCol;
    }

    /**
     * @return Right column.
     */
    public String rightColumn() {
        return rightCol;
    }

    /**
     * @return Wheter this is a cross-join.
     */
    public boolean cross() {
        return cross;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = leftAlias.hashCode();

        res = 31 * res + rightAlias.hashCode();
        res = 31 * res + leftCol.hashCode();
        res = 31 * res + rightCol.hashCode();
        res = 31 * res + Boolean.hashCode(cross);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj instanceof PartitionJoinCondition) {
            PartitionJoinCondition other = (PartitionJoinCondition)obj;

            return F.eq(leftAlias, other.leftAlias) && F.eq(rightAlias, other.rightAlias) &&
                F.eq(leftCol, other.leftCol) && F.eq(rightCol, other.rightCol) && F.eq(cross, other.cross);
        }

        return false;
    }
}
