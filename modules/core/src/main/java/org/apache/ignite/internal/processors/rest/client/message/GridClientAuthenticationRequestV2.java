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

package org.apache.ignite.internal.processors.rest.client.message;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * Client authentication request with user attributes.
 */
public class GridClientAuthenticationRequestV2 extends GridClientAuthenticationRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** User attributes. */
    private Map<String, String> userAttrs;

    /**
     * @return User attributes.
     */
    @Nullable public Map<String, String> userAttributes() {
        return userAttrs;
    }

    /**
     * @param userAttrs User attributes.
     */
    public void userAttributes(Map<String, String> userAttrs) {
        this.userAttrs = userAttrs;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeMap(out, userAttrs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        userAttrs = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientAuthenticationRequestV2.class, this, super.toString());
    }
}
