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

package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CompactablePageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Dummy PageIO implementation. For test purposes only.
 */
public class DummyPageIO extends PageIO implements CompactablePageIO {
    /** */
    public static final IOVersions<DummyPageIO> VERSIONS = new IOVersions<>(new DummyPageIO());

    /** */
    public DummyPageIO() {
        super(Short.MAX_VALUE, 1);

        PageIO.registerTest(this);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("DummyPageIO [\n");
        sb.a("addr=").a(addr).a(", ");
        sb.a("pageSize=").a(addr);
        sb.a("\n]");
    }

    /** {@inheritDoc} */
    @Override public void compactPage(ByteBuffer page, ByteBuffer out, int pageSize) {
        copyPage(page, out, pageSize);
    }

    /** {@inheritDoc} */
    @Override public void restorePage(ByteBuffer p, int pageSize) {
        assert p.isDirect();
        assert p.position() == 0;
        assert p.limit() == pageSize;
    }
}
