/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.sql.calcite.expressions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.IgniteException;

/**
 * TODO: Add class description.
 */
public class BinaryCondition implements Condition {
    private SqlKind kind;
    private Expression left;
    private Expression right;

    public BinaryCondition() {
    }

    public BinaryCondition(SqlKind kind, Expression left, Expression right) {
        this.kind = kind;
        this.left = left;
        this.right = right;
    }

    // TODO: Nulls handling
    @Override public Boolean evaluate(List row) {
        switch (kind) {
            case AND:
                return ((Boolean)left.evaluate(row)) && ((Boolean)right.evaluate(row));
            case OR:
                return ((Boolean)left.evaluate(row)) || ((Boolean)right.evaluate(row));
            case EQUALS:
                return isEquals(row);
            case GREATER_THAN:
                return compare(row, true);
            case GREATER_THAN_OR_EQUAL:
                return compare(row, true) || isEquals(row);
            case LESS_THAN:
                return compare(row, false);
            case LESS_THAN_OR_EQUAL:
                return compare(row, false) || isEquals(row);

            default:
                throw new IgniteException("Unsupported type of condition: " + kind);
        }
    }

    // TODO something more meaningful
    private Boolean compare(List row, boolean greater) {
        Object l = left.evaluate(row);
        Object r = right.evaluate(row);

        if (l instanceof Comparable && r instanceof Comparable && r.getClass() == l.getClass())
            return ((Comparable)l).compareTo(r) > 0 == greater;

        if (l instanceof Number && r instanceof Number) {
            return ((Number)l).longValue() > ((Number)r).longValue() == greater; // TODO decimal types
        }

        throw new IgniteException("Can not compare types: " + l.getClass() + " and " + r.getClass());
    }

    private boolean isEquals(List row) {
        Object l = left.evaluate(row);
        Object r = right.evaluate(row);

        if (l instanceof Number && r instanceof Number) {
            return ((Number)l).longValue() == ((Number)r).longValue(); // TODO decimal types
        }

        return Objects.equals(l, r);
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(kind.ordinal());
        out.writeObject(left);
        out.writeObject(right);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        kind = SqlKind.values()[in.readInt()];
        left = (Expression)in.readObject();
        right = (Expression)in.readObject();
    }

    @Override public String toString() {
        return left.toString() + String.valueOf(kind) + right.toString();
    }
}
