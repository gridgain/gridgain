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

import java.sql.Timestamp;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueTimestampTimeZone;

/**
 * Inline index column implementation for inlining {@link Timestamp} values.
 */
public class TimestampLocalTimeZoneInlineIndexColumn extends AbstractInlineIndexColumn {
    /**
     * @param col Column.
     */
    public TimestampLocalTimeZoneInlineIndexColumn(Column col) {
        super(col, Value.TIMESTAMP_TZ, (short)16);
    }

    /** {@inheritDoc} */
    @Override protected int compare0(long pageAddr, int off, Value v, int type) {
        if (type() != type)
            return COMPARE_UNSUPPORTED;

        ValueTimestampTimeZone ts = (ValueTimestampTimeZone)v.convertTo(type);

        long val1 = PageUtils.getLong(pageAddr, off + 1);
        long val2 = ts.getDateValue();

        assert ts.getTimeZoneOffsetSeconds() == 0;

        int c = Integer.signum(Long.compare(val1, val2));

        if (c != 0)
            return c;

        long nanos1 = PageUtils.getLong(pageAddr, off + 9);
        long nanos2 = ts.getTimeNanos();

        return Integer.signum(Long.compare(nanos1, nanos2));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Value val, int maxSize) {
        assert type() == val.getValueType();

        ValueTimestampTimeZone ts = (ValueTimestampTimeZone)val;

        PageUtils.putByte(pageAddr, off, (byte)val.getValueType());
        PageUtils.putLong(pageAddr, off + 1, ts.getDateValue());
        PageUtils.putLong(pageAddr, off + 9, ts.getTimeNanos());

        return size() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Value get0(long pageAddr, int off) {
        return ValueTimestampTimeZone.fromDateValueAndNanos(
            PageUtils.getLong(pageAddr, off + 1),
            PageUtils.getLong(pageAddr, off + 9),
            (short)0);
    }

    /** {@inheritDoc} */
    @Override protected int inlineSizeOf0(Value val) {
        assert val.getType().getValueType() == type();

        return size() + 1;
    }
}
