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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Extended binary object interface.
 */
public interface BinaryObjectEx extends BinaryObject {
    /**
     * @return Type ID.
     */
    public int typeId();

    /**
     * Get raw type.
     *
     * @return Raw type
     * @throws BinaryObjectException If failed.
     */
    @Nullable public BinaryType rawType() throws BinaryObjectException;

    /**
     * Check if flag set.
     *
     * @param flag flag to check.
     * @return {@code true} if flag is set, {@code false} otherwise.
     */
    public boolean isFlagSet(short flag);

    /**
     * Returns a string representation of deserialized underlying object.
     * This method may return {@code null} if the underlying object cannot be deserialized for some reason.
     *
     * @return String representation of the underlying object.
     */
    @Nullable String deserializedRepresentation();

    /**
     * Returns a string representation of this binary object.
     * This method is equivalent to {@code toString(GridToStringBuilder.getSensitiveDataLogging()}.
     *
     * @return String representation of this binary object.
     */
    @Override String toString();

    /**
     * Returns a string representation of this binary object based on the provided sensitive data logging flag.
     *
     * @param sensitiveDataLogging Sensitive data logging flag.
     * @return String representation of this binary object.
     * @see GridToStringBuilder.SensitiveDataLogging
     */
    @Nullable String toString(GridToStringBuilder.SensitiveDataLogging sensitiveDataLogging);
}
