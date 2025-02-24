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

package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class VisorDrCleanupTreeTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Visor task messages. */
    private List<String> messages;

    /** {@code True} if Visor task was successfully completed, {@code false} otherwise. */
    private boolean success;

    /**
     * Default constructor.
     */
    public VisorDrCleanupTreeTaskResult() {
    }

    /**
     * @param success {@code True} if task was successfully completed, {@code false} otherwise.
     * @param message Task result message.
     */
    VisorDrCleanupTreeTaskResult(boolean success, String message) {
        this.success = success;
        this.messages = Collections.singletonList(message);
    }

    /**
     * @param failures Task failures.
     */
    VisorDrCleanupTreeTaskResult(List<String> failures) {
        assert !failures.isEmpty();

        this.success = false;
        this.messages = failures;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(success);
        U.writeCollection(out, messages);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        success = in.readBoolean();
        messages = U.readList(in);
    }

    public boolean isSuccess() {
        return success;
    }

    public List<String> messages() {
        return messages;
    }
}
