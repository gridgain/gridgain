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

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.compress.CompressionConfigurationTest;
import org.apache.ignite.internal.processors.compress.CompressionProcessorTest;
import org.apache.ignite.internal.processors.compress.DiskPageCompressionIntegrationAsyncTest;
import org.apache.ignite.internal.processors.compress.DiskPageCompressionIntegrationTest;
import org.apache.ignite.internal.processors.compress.FileSystemUtilsTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFAULT_DATA_STORAGE_PAGE_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFAULT_DISK_PAGE_COMPRESSION;
import static org.apache.ignite.configuration.DiskPageCompression.ZSTD;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsCompressionTestSuite {
    /**
     * @return Suite.
     */
    public static List<Class<?>> suite() {
        List<Class<?>> suite = new ArrayList<>();

        suite.add(CompressionConfigurationTest.class);
        suite.add(CompressionProcessorTest.class);
        suite.add(FileSystemUtilsTest.class);
        suite.add(DiskPageCompressionIntegrationTest.class);
        suite.add(DiskPageCompressionIntegrationAsyncTest.class);

        enableCompressionByDefault();
        IgnitePdsTestSuite.addRealPageStoreTests(suite, null);

        return suite;
    }

    /**
     */
    static void enableCompressionByDefault() {
        System.setProperty(IGNITE_DEFAULT_DISK_PAGE_COMPRESSION, ZSTD.name());
        System.setProperty(IGNITE_DEFAULT_DATA_STORAGE_PAGE_SIZE, String.valueOf(8 * 1024));
    }
}
