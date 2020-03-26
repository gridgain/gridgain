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

package org.apache.ignite.internal.visor.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.util.IgniteUtils.readCollection;
import static org.apache.ignite.internal.util.IgniteUtils.writeCollection;

/**
 * Result of checking size cache and index.
 */
public class ValidateIndexesCheckSizeResult extends VisorDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Cache size. */
    private long cacheSize;

    /** Issues. */
    @GridToStringExclude
    private Collection<ValidateIndexesCheckSizeIssue> issues;

    /**
     * Default constructor.
     */
    public ValidateIndexesCheckSizeResult() {
        //Default constructor required for Externalizable.
    }

    /**
     * Constructor.
     *
     * @param cacheSize Cache size.
     * @param issues Issues.
     */
    public ValidateIndexesCheckSizeResult(long cacheSize, Collection<ValidateIndexesCheckSizeIssue> issues) {
        this.cacheSize = cacheSize;
        this.issues = issues;
    }

    /**
     * Return issues when checking size of cache and index.
     *
     * @return Issues when checking size of cache and index.
     */
    public Collection<ValidateIndexesCheckSizeIssue> issues() {
        return issues == null ? emptyList() : issues;
    }

    /**
     * Return cache size.
     *
     * @return Cache size.
     */
    public long cacheSize() {
        return cacheSize;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(cacheSize);
        writeCollection(out, issues);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        cacheSize = in.readLong();
        issues = readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ValidateIndexesCheckSizeResult.class, this);
    }
}
