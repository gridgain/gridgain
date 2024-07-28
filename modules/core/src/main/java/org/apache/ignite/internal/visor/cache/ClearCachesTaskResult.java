/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/** Result of {@link ClearCachesTask}. */
public class ClearCachesTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** List of cleared caches. */
    private List<String> clearedCaches;

    /** List of non-existent caches. */
    private List<String> nonExistentCaches;

    /** */
    public ClearCachesTaskResult(List<String> clearedCaches, List<String> nonExistentCaches) {
        this.clearedCaches = clearedCaches;
        this.nonExistentCaches = nonExistentCaches;
    }

    /** */
    public ClearCachesTaskResult() {
        // No-op.
    }

    /** @return List of cleared caches. */
    public List<String> clearedCaches() {
        return clearedCaches;
    }

    /** @return List of non-existent caches. */
    public List<String> nonExistentCaches() {
        return nonExistentCaches;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, clearedCaches);
        U.writeCollection(out, nonExistentCaches);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        clearedCaches = U.readList(in);
        nonExistentCaches = U.readList(in);
    }
}
