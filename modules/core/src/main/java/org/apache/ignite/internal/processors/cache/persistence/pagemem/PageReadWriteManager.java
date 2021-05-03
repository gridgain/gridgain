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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.PageStore;

/** */
public interface PageReadWriteManager {
    /**
     * Reads a page for the given cache ID. Cache ID may be {@code 0} if the page is a meta page.
     *
     * @param grpId Cache group ID.
     * @param pageId PageID to read.
     * @param pageBuf Page buffer to write to.
     * @param keepCrc Keep CRC flag.
     * @throws IgniteCheckedException If failed to read the page.
     */
    public void read(int grpId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteCheckedException;

    /**
     * Writes the page for the given cache ID. Cache ID may be {@code 0} if the page is a meta page.
     *
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write.
     * @throws IgniteCheckedException If failed to write page.
     */
    public PageStore write(int grpId, long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteCheckedException;

    /**
     * Allocates a page for the given page space.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID. Used only if {@code flags} is equal to {@link PageMemory#FLAG_DATA}.
     * @param flags Page allocation flags.
     * @return Allocated page ID.
     * @throws IgniteCheckedException If IO exception occurred while allocating a page ID.
     */
    public long allocatePage(int grpId, int partId, byte flags) throws IgniteCheckedException;
}
