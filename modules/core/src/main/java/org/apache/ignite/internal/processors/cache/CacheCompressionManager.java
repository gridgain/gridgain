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

package org.apache.ignite.internal.processors.cache;

import java.io.File;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.compress.CompressionProcessor.checkCompressionLevelBounds;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.getDefaultCompressionLevel;

/**
 * Cache compression manager.
 */
public class CacheCompressionManager extends GridCacheManagerAdapter {
    /** */
    private DiskPageCompression diskPageCompression;

    /** */
    private int diskPageCompressLevel;

    /** */
    private CompressionProcessor compressProc;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        compressProc = cctx.kernalContext().compress();

        CacheConfiguration cfg = cctx.config();

        diskPageCompression = cctx.kernalContext().config().isClientMode() ? null : cfg.getDiskPageCompression();

        if (diskPageCompression != null) {
            if (!cctx.dataRegion().config().isPersistenceEnabled())
                throw new IgniteCheckedException("Disk page compression makes sense only with enabled persistence.");

            Integer lvl = cfg.getDiskPageCompressionLevel();
            diskPageCompressLevel = lvl != null ?
                checkCompressionLevelBounds(lvl, diskPageCompression) :
                getDefaultCompressionLevel(diskPageCompression);

            DataStorageConfiguration dsCfg = cctx.kernalContext().config().getDataStorageConfiguration();

            File dbPath = cctx.kernalContext().pdsFolderResolver().resolveFolders().persistentStoreRootPath();

            assert dbPath != null;

            compressProc.checkPageCompressionSupported(dbPath.toPath(), dsCfg.getPageSize());

            if (log.isInfoEnabled()) {
                log.info("Disk page compression is enabled [cache=" + cctx.name() +
                    ", compression=" + diskPageCompression + ", level=" + diskPageCompressLevel + "]");
            }
        }
    }

    /**
     * @param page Page buffer.
     * @param store Page store.
     * @return Compressed or the same buffer.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer compressPage(ByteBuffer page, PageStore store) throws IgniteCheckedException {
        if (diskPageCompression == null)
            return page;

        int blockSize = store.getBlockSize();

        if (blockSize <= 0)
            throw new IgniteCheckedException("Failed to detect storage block size on " + U.osString());

        return compressProc.compressPage(page, store.getPageSize(), blockSize, diskPageCompression, diskPageCompressLevel);
    }
}
