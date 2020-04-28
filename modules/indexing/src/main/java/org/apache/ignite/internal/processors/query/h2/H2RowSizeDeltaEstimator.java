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
package org.apache.ignite.internal.processors.query.h2;

import java.util.BitSet;
import org.apache.ignite.internal.util.typedef.T2;
import org.h2.engine.Constants;
import org.h2.expression.aggregate.AggregateData;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * Helper class for estimating difference between row sizes.
 */
public class H2RowSizeDeltaEstimator {
    /** Null object. */
    private static final BitSet NULL_SET = new BitSet(0);

    /** Set of H2 types wit variable length. */
    private static final BitSet VARIABLE_LENGTH_TYPES = new BitSet(Value.TYPE_COUNT);

    static {
        VARIABLE_LENGTH_TYPES.set(Value.DECIMAL);
        VARIABLE_LENGTH_TYPES.set(Value.BYTES);
        VARIABLE_LENGTH_TYPES.set(Value.STRING);
        VARIABLE_LENGTH_TYPES.set(Value.STRING_IGNORECASE);
        VARIABLE_LENGTH_TYPES.set(Value.ARRAY);
        VARIABLE_LENGTH_TYPES.set(Value.RESULT_SET);
        VARIABLE_LENGTH_TYPES.set(Value.JAVA_OBJECT);
        VARIABLE_LENGTH_TYPES.set(Value.ROW);
    }

    /** Set of columns with variable length in key. */
    private BitSet keyVarFields;

    /** Set of columns with variable length in value. */
    private BitSet valVarFields;

    /** Total length of fixed size key columns in bytes . */
    private int keyFixedFieldsSize;

    /** Total length of fixed size value columns in bytes. */
    private int valFixedFieldsSize;

    /** */
    public long calculateMemoryDelta(ValueRow distinctRowKey, Object[] oldRow, Object[] newRow) {
        if (distinctRowKey != null && keyVarFields == null)
            initializeKey(distinctRowKey);

        if (valVarFields == null && (oldRow != null || newRow != null))
            initializeValue(oldRow, newRow);

        int keySize = distinctRowKey == null ? 0 : keyRowSize(distinctRowKey);
        int oldSize = oldRow == null ? 0 : valRowSize(oldRow);
        int newSize = newRow == null ? 0 : valRowSize(newRow);

        long delta = newSize - oldSize;

        if (distinctRowKey != null) {
            if (oldRow == null)
                delta += keySize;
            else if (newRow == null)
                delta -= keySize;
        }

        return delta;
    }

    /** */
    private int keyRowSize(ValueRow keyRow) {
        int varFieldsSize = keyVarFields == NULL_SET ? 0 : variableFieldsRowSize(keyRow.getList(), keyVarFields);

        return varFieldsSize + keyFixedFieldsSize;
    }

    /** */
    private int valRowSize(Object[] row) {
        int varFieldsSize = valVarFields == NULL_SET ? 0 : variableFieldsRowSize(row, valVarFields);

        return varFieldsSize + valFixedFieldsSize;
    }

    /** */
    private int variableFieldsRowSize(Object[] row, BitSet fields) {
        if (fields == null)
            return 0;

        int varFieldsSize = 0;

        for (int i = fields.nextSetBit(0); i != -1; i = fields.nextSetBit(i + 1)) {
            Object v = row[i];
            if (v instanceof Value)
                varFieldsSize += ((Value)row[i]).getMemory();
            else if (v instanceof AggregateData)
                varFieldsSize += ((AggregateData)row[i]).getMemory();
        }

        return varFieldsSize;
    }

    /** */
    private void initializeValue(Object[] oldRow, Object[] newRow) {
        Object[] row = oldRow == null ? newRow : oldRow;
        assert  row != null;

        T2<Integer, BitSet> analyzedFields = analyzeFixedAndVariableFields(row);

        valFixedFieldsSize = analyzedFields.getKey();
        valVarFields = analyzedFields.getValue();
    }

    /** */
    private void initializeKey(ValueRow distinctRowKey) {
        Value[] row = distinctRowKey.getList();

        T2<Integer, BitSet> analyzedFields = analyzeFixedAndVariableFields(row);

        keyFixedFieldsSize = analyzedFields.getKey();
        keyVarFields = analyzedFields.getValue();
    }

    /** */
    private T2<Integer, BitSet> analyzeFixedAndVariableFields(Object[] row) {
        BitSet varLenCols = new BitSet(row.length);
        int fixedSize = Constants.MEMORY_ARRAY + row.length * Constants.MEMORY_POINTER;;

        for (int i = 0; i < row.length; i++) {
            Object v = row[i];
            int type = v instanceof Value ? ((Value)v).getValueType() : -1;
            if (type < 0 || VARIABLE_LENGTH_TYPES.get(type))
                varLenCols.set(i);
            else if (type == Value.INT || type == Value.LONG)
                fixedSize += Value.DEFAULT_MEMORY; // Ignore caching for int and long types.
            else
                fixedSize += ((Value)v).getMemory();
        }

        if (varLenCols.cardinality() == 0)
            varLenCols = NULL_SET; // Use dummy set if all columns are fixed.

        return new T2<>(fixedSize, varLenCols);
    }
}
