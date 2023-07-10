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

package org.apache.ignite.jdbc.thin;

/**
 * Helper for getting erorr messages
 */
public class JdbcThinBulkLoadErrorMessageHelper {

    /**
     * @return wrong file name message
     */
    public static String getWrongFileNameMessage(boolean isLegacyBulkLoad) {
        return (isLegacyBulkLoad) ? "Failed to read file: 'nonexistent'" : "nonexistent (No such file or directory)";
    }

    /**
     * @return unmatched quote message
     */
    public static String getUnmatchedQuoteMessage(boolean isLegacyBulkLoad) {
        return (isLegacyBulkLoad) ? "Unmatched quote found at the end of line" : "Unterminated quoted field at end of CSV line";
    }

    /**
     * @return unmatched quote field message
     */
    public static String getUnmatchedQuoteFieldMessage(boolean isLegacyBulkLoad) {
        return (isLegacyBulkLoad) ? "Unexpected quote in the field, line" : "Unterminated quoted field at end of CSV line";
    }
}
