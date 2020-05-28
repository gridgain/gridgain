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

package org.apache.ignite.development.utils.indexreader;

import java.io.File;

/**
 * Container of file and position (offset) in it.
 */
public class FilePosition {
    /** Position in file or offset. */
    private final long position;

    /** File. */
    private final File file;

    /**
     * Constructor.
     *
     * @param position Position in file or offset.
     * @param file File.
     */
    public FilePosition(long position, File file) {
        this.position = position;
        this.file = file;
    }

    /**
     * Return position in file or offset.
     *
     * @return Position in file or offset.
     */
    public long position() {
        return position;
    }

    /**
     * Return file.
     *
     * @return File.
     */
    public File file() {
        return file;
    }

    /**
     * Return approximate instance size.
     *
     * @return Approximate instance size.
     */
    public static long instanceSize() {
        return IndexReaderUtils.objectSize(IndexReaderUtils.linkSize() + 8);
    }
}
