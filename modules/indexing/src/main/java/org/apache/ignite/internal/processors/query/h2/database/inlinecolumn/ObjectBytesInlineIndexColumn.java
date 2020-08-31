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

import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueJavaObject;

/**
 * Inline index column implementation for inlining Java objects as byte array.
 */
public class ObjectBytesInlineIndexColumn extends BytesInlineIndexColumn {
    /**
     * @param col Column.
     * @param compareBinaryUnsigned Compare binary unsigned.
     */
    public ObjectBytesInlineIndexColumn(Column col, boolean compareBinaryUnsigned) {
        super(col, Value.JAVA_OBJECT, compareBinaryUnsigned);
    }

    /** {@inheritDoc} */
    @Override protected Value get0(long pageAddr, int off) {
        return ValueJavaObject.getNoCopy(null, readBytes(pageAddr, off), null);
    }
}
