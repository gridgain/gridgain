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

package org.apache.ignite.internal.processors.query.h2.database.inlinecolumn;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.value.Value;

/**
 * Inline index column implementation for inlining decimal values.
 */
public class DecimalInlineIndexColumn extends AbstractInlineIndexColumn {
    /**
     * @param col Column.
     */
    public DecimalInlineIndexColumn(Column col) {
        super(col, Value.DECIMAL, (short)4);
    }

    /** {@inheritDoc} */
    @Override protected int compare0(long pageAddr, int off, Value v, int type) {
        if (type() != type)
            return COMPARE_UNSUPPORTED;

        float val1 = Float.intBitsToFloat(PageUtils.getInt(pageAddr, off + 1));
        float val2 = v.getFloat();

        int res = Integer.signum(Float.compare(val1, val2));
        return res == 0 ? CANT_BE_COMPARE : res;
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Value val, int maxSize) {
        assert type() == val.getValueType();

        PageUtils.putByte(pageAddr, off, (byte)val.getValueType());
        PageUtils.putInt(pageAddr, off + 1, Float.floatToIntBits(val.getFloat()));

        return size() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Value get0(long pageAddr, int off) {
        // can't be sure about completeness of the stored value, better to return null to prevent further incorrect work
        return null;
    }

    /** {@inheritDoc} */
    @Override protected int inlineSizeOf0(Value val) {
        assert val.getType().getValueType() == type();

        return size() + 1;
    }
}
