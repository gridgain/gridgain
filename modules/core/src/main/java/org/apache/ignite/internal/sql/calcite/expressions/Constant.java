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
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

/**
 * TODO: Add class description.
 */
public class Constant implements Expression {

    private Object constant;

    public Constant() {
    }

    public Constant(RexLiteral literal) {
        this.constant = convertValueIfNeeded(literal.getValue(), literal.getType());
    }

    @Override public Object evaluate(List row) {
        return constant;
    }

    private Object convertValueIfNeeded(Object val, RelDataType type) {
        if (val instanceof BigDecimal)
            return ((BigDecimal)val).longValue(); // TODO DECIMAL types

        return val;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(constant);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        constant = in.readObject();
    }

    @Override public String toString() {
        return "{" +
            constant +
            '}';
    }
}
