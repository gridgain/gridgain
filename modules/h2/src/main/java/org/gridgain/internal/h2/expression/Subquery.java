/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.expression;

import java.util.ArrayList;
import org.gridgain.internal.h2.command.dml.Query;
import org.gridgain.internal.h2.api.ErrorCode;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.message.DbException;
import org.gridgain.internal.h2.result.ResultInterface;
import org.gridgain.internal.h2.table.ColumnResolver;
import org.gridgain.internal.h2.table.TableFilter;
import org.gridgain.internal.h2.value.TypeInfo;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueNull;
import org.gridgain.internal.h2.value.ValueRow;

/**
 * A query returning a single value.
 * Subqueries are used inside other statements.
 */
public class Subquery extends Expression {

    private final Query query;
    private Expression expression;

    public Subquery(Query query) {
        this.query = query;
    }

    @Override
    public Value getValue(Session session) {
        query.setSession(session);
        ResultInterface lastResult = query.getLastResult();
        ResultInterface result = query.query(2);
        try {
            Value v;
            if (!result.next()) {
                v = ValueNull.INSTANCE;
            } else {
                Value[] values = result.currentRow();
                if (result.getVisibleColumnCount() == 1) {
                    v = values[0];
                } else {
                    v = ValueRow.get(values);
                }
                if (result.hasNext()) {
                    throw DbException.get(ErrorCode.SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW);
                }
            }
            return v;
        }
        finally {
            // Do not close the very first lastResult in the case when caching is turned on.
            // Otherwise close all lastResults.
            if (lastResult != null || query.ignoreCaching())
                result.close();
        }
    }

    @Override
    public TypeInfo getType() {
        return getExpression().getType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        query.mapColumns(resolver, level + 1);
    }

    @Override
    public Expression optimize(Session session) {
        session.optimizeQueryExpression(query);
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        query.setEvaluatable(tableFilter, b);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        return builder.append('(').append(query.getPlanSQL(alwaysQuote)).append(')');
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        query.updateAggregate(session, stage);
    }

    private Expression getExpression() {
        if (expression == null) {
            ArrayList<Expression> expressions = query.getExpressions();
            int columnCount = query.getColumnCount();
            if (columnCount == 1) {
                expression = expressions.get(0);
            } else {
                Expression[] list = new Expression[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    list[i] = expressions.get(i);
                }
                expression = new ExpressionList(list, false);
            }
        }
        return expression;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public int getCost() {
        return query.getCostAsExpression();
    }

    @Override
    public Expression[] getExpressionColumns(Session session) {
        return getExpression().getExpressionColumns(session);
    }
}
