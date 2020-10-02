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

import java.sql.Time;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueTime;

/**
 * Inline index column implementation for inlining {@link Time} values.
 */
public class TimeInlineIndexColumn extends AbstractInlineIndexColumn {
    /**
     * @param col Column.
     */
    public TimeInlineIndexColumn(Column col) {
        super(col, Value.TIME, (short)8);
    }

    /** {@inheritDoc} */
    @Override protected int compare0(long pageAddr, int off, Value v, int type) {
        if (type() != type)
            return COMPARE_UNSUPPORTED;

        long val1 = PageUtils.getLong(pageAddr, off + 1);
        long val2 = ((ValueTime)v.convertTo(type)).getNanos();

        return Integer.signum(Long.compare(val1, val2));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Value val, int maxSize) {
        assert type() == val.getValueType();

        PageUtils.putByte(pageAddr, off, (byte)val.getValueType());
        PageUtils.putLong(pageAddr, off + 1, ((ValueTime)val).getNanos());

        return size() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Value get0(long pageAddr, int off) {
        return ValueTime.fromNanos(PageUtils.getLong(pageAddr, off + 1));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSizeOf0(Value val) {
        assert val.getType().getValueType() == type();

        return size() + 1;
    }
}
