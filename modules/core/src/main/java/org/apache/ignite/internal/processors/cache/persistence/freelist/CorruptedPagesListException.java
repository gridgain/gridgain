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
package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.internal.processors.cache.persistence.AbstractCorruptedPersistenceException;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/** */
public class CorruptedPagesListException extends AbstractCorruptedPersistenceException {
    /**
     * @param msg   Message.
     * @param cause Cause.
     * @param pages (groupId, pageId) pairs for pages that might be corrupted.
     */
    protected CorruptedPagesListException(
        String msg,
        @Nullable Throwable cause,
        T2<Integer, Long>[] pages
    ) {
        super(msg, cause, pages);
    }
}
