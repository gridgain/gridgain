/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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
 * A placeholder for bulk load Parquet format parser options.
 */
public class BulkLoadParquetFormat implements BulkLoadFormat {
    /** regex pattern for matching files */
    private String pattern;

    /**
     * Sets the pattern for matching files.
     * Must be a regex expression
     */
    public void pattern(String pattern) {
        this.pattern = pattern;
    }

    /**
     * Returns the pattern for matching files.
     */
    public String pattern() {
        return pattern;
    }

    @Override public String name() {
        return "Parquet";
    }
}
