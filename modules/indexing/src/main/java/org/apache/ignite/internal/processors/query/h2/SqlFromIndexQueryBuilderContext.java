package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.SqlBuilderContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.gridgain.internal.h2.table.Column;

class SqlFromIndexQueryBuilderContext implements SqlBuilderContext {
    private final H2TableDescriptor descriptor;
    private final Column column;

    SqlFromIndexQueryBuilderContext(H2TableDescriptor descriptor, String field) {
        this.descriptor = descriptor;

        column = getColumn(field, descriptor.table());
    }

    @Override public String columnName() {
        return "\"" + column.getName() + "\"";
    }

    @Override public boolean nullable() {
        return column.isNullable();
    }

    private Column getColumn(String field, GridH2Table table) {
        String upperCaseField = field.toUpperCase();

        if (table.doesColumnExist(upperCaseField))
            return table.getColumn(upperCaseField);

        if (table.doesColumnExist(field))
            return table.getColumn(field);

        throw new IgniteException("Column \"" + upperCaseField + "\" not found.");
    }
}
