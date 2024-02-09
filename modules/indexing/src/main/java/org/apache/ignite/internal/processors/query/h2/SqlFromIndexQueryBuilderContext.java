package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.SqlBuilderContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.gridgain.internal.h2.table.Column;

class SqlFromIndexQueryBuilderContext implements SqlBuilderContext {
    private final H2TableDescriptor descriptor;
    private final Column column;
    private final List<Object> arguments;

    SqlFromIndexQueryBuilderContext(H2TableDescriptor descriptor, String field, List<Object> arguments) {
        this.descriptor = descriptor;
        this.arguments = arguments;

        column = getColumn(field, descriptor.table());
    }

    @Override public String columnName() {
        return "\"" + column.getName() + "\"";
    }

    @Override public boolean nullable() {
        return column.isNullable();
    }

    @Override public void addArgument(Object arg) {
        arguments.add(arg);
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
