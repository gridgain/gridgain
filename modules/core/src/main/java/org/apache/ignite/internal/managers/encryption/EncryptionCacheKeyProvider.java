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

package org.apache.ignite.internal.managers.encryption;

import org.jetbrains.annotations.Nullable;

/**
 * Provider for cache's encryption keys.
 */
public interface EncryptionCacheKeyProvider {
    /**
     * Returns group encryption key, that was set for writing.
     *
     * @param grpId Cache group ID.
     * @return Group encryption key with ID, that was set for writing.
     */
    @Nullable GroupKey getActiveKey(int grpId);

    /**
     * Returns group encryption key with specified ID.
     *
     * @param grpId Cache group ID.
     * @param keyId Encryption key ID.
     * @return Group encryption key.
     */
    @Nullable GroupKey groupKey(int grpId, int keyId);
}
