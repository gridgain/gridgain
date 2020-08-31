/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.expression.condition;

import org.gridgain.internal.h2.command.dml.Query;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.expression.Expression;
import org.gridgain.internal.h2.expression.ExpressionVisitor;
import org.gridgain.internal.h2.result.ResultInterface;
import org.gridgain.internal.h2.table.ColumnResolver;
import org.gridgain.internal.h2.table.TableFilter;
import org.gridgain.internal.h2.util.StringUtils;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueBoolean;

/**
 * An 'exists' condition as in WHERE EXISTS(SELECT ...)
 */
public class ConditionExists extends Condition {

    private final Query query;

    public ConditionExists(Query query) {
        this.query = query;
    }

    @Override
    public Value getValue(Session session) {
        query.setSession(session);
        ResultInterface lastResult = query.getLastResult();
        ResultInterface result = query.query(1);
        try {
            session.addTemporaryResult(result);
            boolean r = result.hasNext();
            return ValueBoolean.get(r);
        }
        finally {
            // Do not close the very first lastResult in the case when caching is turned on. Otherwise close all results.
            if (lastResult != null || query.ignoreCaching())
                result.close();
        }
    }

    @Override
    public Expression optimize(Session session) {
        session.optimizeQueryExpression(query);
        return this;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append("EXISTS(\n");
        return StringUtils.indent(builder, query.getPlanSQL(alwaysQuote), 4, false).append(')');
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        // TODO exists: is it allowed that the subquery contains aggregates?
        // probably not
        // select id from test group by id having exists (select * from test2
        // where id=count(test.id))
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        query.mapColumns(resolver, level + 1);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        query.setEvaluatable(tableFilter, b);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return query.getCostAsExpression();
    }

}
