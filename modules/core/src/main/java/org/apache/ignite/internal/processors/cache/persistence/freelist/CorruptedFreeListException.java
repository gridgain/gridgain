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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import java.util.Collection;
import org.apache.ignite.internal.processors.cache.persistence.AbstractCorruptedPersistenceException;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 * Exception to distinguish {@link AbstractFreeList} broken invariants.
 */
public class CorruptedFreeListException extends AbstractCorruptedPersistenceException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Group id.
     * @param pageIds Ids of pages that are possibly corrupted.
     */
    public CorruptedFreeListException(String msg, @Nullable Throwable cause, int grpId, Collection<Long> pageIds) {
        this(msg, cause, grpId, pageIds.stream().mapToLong(Long::longValue).toArray());
    }

    /**
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Group id.
     * @param pageIds Ids of pages that are possibly corrupted.
     */
    public CorruptedFreeListException(String msg, @Nullable Throwable cause, int grpId, long... pageIds) {
        this(msg, cause, toPagesArray(grpId, pageIds));
    }

    /**
     * @param msg Message.
     * @param cause Cause.
     * @param pages (groupId, pageId) pairs for pages that might be corrupted.
     */
    public CorruptedFreeListException(String msg,
        @Nullable Throwable cause,
        T2<Integer, Long>[] pages
    ) {
        super(msg, cause, pages);
    }
}
