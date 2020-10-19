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

package org.apache.ignite.internal.processors.metric.sql;

import java.util.Collections;
import java.util.Iterator;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlAbstractLocalSystemView;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.result.Row;
import org.gridgain.internal.h2.result.SearchRow;
import org.gridgain.internal.h2.value.Value;

/**
 * Sql view for exporting metrics.
 */
class MetricRegistryLocalSystemView extends SqlAbstractLocalSystemView {
    /** Metric registry. */
    private ReadOnlyMetricManager mreg;

    /**
     * @param ctx Context.
     * @param mreg Metric registry.
     */
    MetricRegistryLocalSystemView(GridKernalContext ctx, ReadOnlyMetricManager mreg) {
        super(SqlViewMetricExporterSpi.SYS_VIEW_NAME, "Ignite metrics",
            ctx,
            newColumn("NAME", Value.STRING),
            newColumn("VALUE", Value.STRING),
            newColumn("DESCRIPTION", Value.STRING));

        this.mreg = mreg;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        return new Iterator<Row>() {
            /** */
            private Iterator<ReadOnlyMetricRegistry> grps = mreg.iterator();

            /** */
            private Iterator<Metric> curr = Collections.emptyIterator();

            /** */
            private boolean advance() {
                while (grps.hasNext()) {
                    ReadOnlyMetricRegistry mreg = grps.next();

                    curr = mreg.iterator();

                    if (curr.hasNext())
                        return true;
                }

                return false;
            }

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                if (curr.hasNext())
                    return true;

                return advance();
            }

            /** {@inheritDoc} */
            @Override public Row next() {
                Metric m = curr.next();

                return createRow(ses, m.name(), m.getAsString(), m.description());
            }
        };
    }
}
