/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class VisorDrCleanupTreeTaskArgs extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Operation. */
    private VisorDrRebuildTreeOperation op;

    /** Caches. */
    private Set<String> caches;

    /** Cache groups. */
    private Set<String> groups;

    /** */
    public VisorDrCleanupTreeTaskArgs() {
    }

    /**
     *
     */
    public VisorDrCleanupTreeTaskArgs(
        VisorDrRebuildTreeOperation op,
        Set<String> caches,
        Set<String> groups
    ) {
        this.op = op;
        this.caches = caches;
        this.groups = groups;
    }

    public Set<String> caches() {
        return caches;
    }

    public Set<String> groups() {
        return groups;
    }

    public VisorDrRebuildTreeOperation op() {
        return op;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, op);
        U.writeCollection(out, caches);
        U.writeCollection(out, groups);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        op = U.readEnum(in, VisorDrRebuildTreeOperation.class);
        caches = U.readSet(in);
        groups = U.readSet(in);
    }
}
