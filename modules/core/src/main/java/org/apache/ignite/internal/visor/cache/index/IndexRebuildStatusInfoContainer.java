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

package org.apache.ignite.internal.visor.cache.index;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.commandline.CommandHandler.EMPTY_GROUP_NAME;

/**
 * Container for index rebuild statu info.
 */
public class IndexRebuildStatusInfoContainer extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Group name. */
    private String groupName;

    /** Cache name. */
    private String cacheName;

    /**
     * Empty constructor required for Serializable.
     */
    public IndexRebuildStatusInfoContainer() {
        // No-op.
    }

    /** */
    public IndexRebuildStatusInfoContainer(CacheConfiguration cfg) {
        assert cfg != null;

        groupName = cfg.getGroupName() == null ? EMPTY_GROUP_NAME : cfg.getGroupName();
        cacheName = cfg.getName();
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, groupName);
        U.writeString(out, cacheName);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        groupName = U.readString(in);
        cacheName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (!(o instanceof IndexRebuildStatusInfoContainer))
            return false;

        IndexRebuildStatusInfoContainer other = (IndexRebuildStatusInfoContainer)o;

        return cacheName.equals(other.cacheName) && groupName.equals(other.groupName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return groupName.hashCode() * 17 + cacheName.hashCode() * 37;
    }

    /**
     * @return Group name.
     */
    public String groupName() {
        return groupName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexRebuildStatusInfoContainer.class, this);
    }
}
