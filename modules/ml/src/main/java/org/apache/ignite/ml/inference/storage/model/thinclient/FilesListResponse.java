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

package org.apache.ignite.ml.inference.storage.model.thinclient;

import java.util.Set;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Response with list of files in directory.
 */
public class FilesListResponse extends ClientResponse {
    /** Files list. */
    private final Set<String> filesList;

    /**
     * Creates an instance of files list response.
     * @param requestId Request id.
     * @param files Files.
     */
    public FilesListResponse(long requestId, Set<String> files) {
        super(requestId);

        this.filesList = files;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeInt(filesList.size());
        for (String fileName : filesList)
            writer.writeString(fileName);
    }

    /**
     * @return Files list.
     */
    Set<String> getFilesList() {
        return filesList;
    }
}
