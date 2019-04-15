/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.table.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter.mergeTableIdentifier;

/**
 * Engine to create reduce table.
 */
public class ReduceTableEngine implements TableEngine {
    /** */
    private static final ThreadLocal<ReduceTableWrapper> CREATED_TBL = new ThreadLocal<>();

    /**
     * Create merge table over the given connection with provided index.
     *
     * @param conn Connection.
     * @param idx Index.
     * @return Created table.
     */
    public static ReduceTableWrapper create(Connection conn, int idx) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + mergeTableIdentifier(idx) +
                "(fake BOOL) ENGINE \"" + ReduceTableEngine.class.getName() + '"');
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }

        ReduceTableWrapper tbl = CREATED_TBL.get();

        assert tbl != null;

        CREATED_TBL.remove();

        return tbl;
    }

    /** {@inheritDoc} */
    @Override public Table createTable(CreateTableData d) {
        assert CREATED_TBL.get() == null;

        ReduceTableWrapper tbl = new ReduceTableWrapper(
            d.schema,
            d.id,
            d.tableName,
            d.persistIndexes,
            d.persistData
        );

        CREATED_TBL.set(tbl);

        return tbl;
    }
}
