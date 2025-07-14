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

package org.apache.ignite.internal.transactions;

import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.jetbrains.annotations.Nullable;

/**
 * Exception thrown whenever transaction tries to insert entry with same mvcc version more than once.
 */
public class IgniteTxDuplicateKeyCheckedException extends TransactionCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key in string representation. */
    String keyStr;

    /** Operation related table name. */
    @Nullable String table;

    /**
     * Creates new duplicate ket exception with given error message.
     *
     * @param key Duplicate key
     */
    public IgniteTxDuplicateKeyCheckedException(KeyCacheObject key) {
        keyStr = key.toString();
    }

    /** Operation related table. */
    public void tableName(String table) {
        this.table = table;
    }

    /** {@inheritDoc} */
    @Override public String getMessage() {
        return "Duplicate key during INSERT [key=" + keyStr + (table == null ? ']' : ", table=" + table + ']');
    }
}
