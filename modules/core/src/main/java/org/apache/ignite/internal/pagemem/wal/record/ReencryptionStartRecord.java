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

package org.apache.ignite.internal.pagemem.wal.record;

import java.util.Map;

/**
 * Logical record to restart reencryption with the latest encryption key.
 */
public class ReencryptionStartRecord extends WALRecord {
    /** Map of reencrypted cache groups with encryption key identifiers. */
    private final Map<Integer, Byte> grps;

    /**
     * @param grps Map of reencrypted cache groups with encryption key identifiers.
     */
    public ReencryptionStartRecord(Map<Integer, Byte> grps) {
        this.grps = grps;
    }

    /**
     * @return Map of reencrypted cache groups with encryption key identifiers.
     */
    public Map<Integer, Byte> groups() {
        return grps;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.REENCRYPTION_START_RECORD;
    }

    /** @return Record data size. */
    public int dataSize() {
        return 4 + ((/*grpId*/4 + /*keyId*/1) * grps.size());
    }
}
