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

package org.apache.ignite.internal.util.io;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * General files manipulation utilities.
 */
public class GridFileUtils {
    /**
     * Copy file
     *
     * @param srcFactory Source factory.
     * @param src Source.
     * @param dstFactory Dst factory.
     * @param dst Dst.
     * @param maxBytes Max bytes.
     */
    public static void copy(
            FileIOFactory srcFactory,
            File src,
            FileIOFactory dstFactory,
            File dst,
            long maxBytes
    ) throws IOException {
        boolean err = true;

        try (FileChannel dstChannel = FileChannel.open(dst.toPath(), CREATE, TRUNCATE_EXISTING, WRITE)) {
            try (FileChannel srcChannel = FileChannel.open(src.toPath(), READ)) {
                long limit = Math.min(srcChannel.size(), maxBytes);
                long position = 0;
                long writtenBytes = 0;

                while (position < limit) {
                    writtenBytes = srcChannel.transferTo(position, limit, dstChannel);

                    position += writtenBytes;
                    limit -= writtenBytes;
                }

                err = false;
            }
        }
        finally {
            if (err)
                dst.delete();
        }
    }
}
