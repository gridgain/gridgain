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

package org.apache.ignite.internal.processors.bulkload;

/**
 * Quoted file path with syntax '../../any.file'.
 */
public class BulkLoadLocationFile implements BulkLoadLocation {

    /** Local file pathname to send from client to server. */
    private String path;

    /**
     * Returns the file pathname.
     *
     * @return The file pathname.
     */
    public String path() {
        return path;
    }

    /**
     * Sets the file pathname.
     *
     * @param path The file pathname.
     */
    public BulkLoadLocationFile path(String path) {
        this.path = path;
        return this;
    }
}
