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

package org.apache.ignite.internal.managers.systemview.walker;

import org.apache.ignite.spi.systemview.view.SqlViewView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link SqlViewView} attributes walker.
 * 
 * @see SqlViewView
 */
public class SqlViewViewWalker implements SystemViewRowAttributeWalker<SqlViewView> {
    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "name", String.class);
        v.accept(1, "schema", String.class);
        v.accept(2, "description", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(SqlViewView row, AttributeWithValueVisitor v) {
        v.accept(0, "name", String.class, row.name());
        v.accept(1, "schema", String.class, row.schema());
        v.accept(2, "description", String.class, row.description());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 3;
    }
}
