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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class ListFieldsQueryCursor<T> implements FieldsQueryCursor<List<?>> {
    private final RelDataType rowType;
    private final Enumerable<T> enumerable;
    private final Function<T, List<?>> converter;

    public ListFieldsQueryCursor(RelDataType rowType, Enumerable<T> enumerable, Function<T, List<?>> converter) {
        this.rowType = rowType;
        this.enumerable = enumerable;
        this.converter = converter;
    }

    @Override public String getFieldName(int idx) {
        return rowType.getFieldList().get(idx).getName();
    }

    @Override public int getColumnsCount() {
        return rowType.getFieldCount();
    }

    @Override public List<List<?>> getAll() {
        return StreamSupport.stream(enumerable.spliterator(), false)
            .map(converter)
            .collect(Collectors.toList());
    }

    @Override public void close() {
    }

    @NotNull @Override public Iterator<List<?>> iterator() {
        return F.iterator(enumerable.iterator(), converter::apply, true);
    }
}
