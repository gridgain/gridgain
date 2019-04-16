/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.inference.reader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.ignite.ml.inference.util.DirectorySerializer;

/**
 * Model reader that reads directory or file and serializes it using {@link DirectorySerializer}.
 */
public class FileSystemModelReader implements ModelReader {
    /** */
    private static final long serialVersionUID = 7370932792669930039L;

    /** Path to the directory. */
    private final String path;

    /**
     * Constructs a new instance of directory model reader.
     *
     * @param path Path to the directory.
     */
    public FileSystemModelReader(String path) {
        this.path = path;
    }

    /** {@inheritDoc} */
    @Override public byte[] read() {
        try {
            File file = Paths.get(path).toFile();
            if (!file.exists())
                throw new IllegalArgumentException("File or directory does not exist [path=" + path + "]");

            if (file.isDirectory())
                return DirectorySerializer.serialize(Paths.get(path));
            else
                return Files.readAllBytes(Paths.get(path));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
