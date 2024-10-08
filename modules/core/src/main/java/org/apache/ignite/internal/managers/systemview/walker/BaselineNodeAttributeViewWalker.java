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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.BaselineNodeAttributeView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link BaselineNodeAttributeView} attributes walker.
 * 
 * @see BaselineNodeAttributeView
 */
public class BaselineNodeAttributeViewWalker implements SystemViewRowAttributeWalker<BaselineNodeAttributeView> {
    /** Filter key for attribute "nodeConsistentId" */
    public static final String NODE_CONSISTENT_ID_FILTER = "nodeConsistentId";

    /** Filter key for attribute "name" */
    public static final String NAME_FILTER = "name";

    /** List of filtrable attributes. */
    private static final List<String> FILTRABLE_ATTRS = Collections.unmodifiableList(F.asList(
        "nodeConsistentId", "name"
    ));

    /** {@inheritDoc} */
    @Override public List<String> filtrableAttributes() {
        return FILTRABLE_ATTRS;
    }

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "nodeConsistentId", String.class);
        v.accept(1, "name", String.class);
        v.accept(2, "value", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(BaselineNodeAttributeView row, AttributeWithValueVisitor v) {
        v.accept(0, "nodeConsistentId", String.class, row.nodeConsistentId());
        v.accept(1, "name", String.class, row.name());
        v.accept(2, "value", String.class, row.value());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 3;
    }
}
