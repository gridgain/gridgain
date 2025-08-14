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

package org.apache.ignite.internal.commandline.walconverter;

import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getPageIO;
import static org.apache.ignite.internal.util.IgniteUtils.byteArray2HexString;

/**
 * Wrapper {@link MetastoreDataRecord} for sensitive data output.
 */
class PageSnapshotRecordWrapper {

    private final PageSnapshot record;
    private final int[] targetIos;

    public PageSnapshotRecordWrapper(PageSnapshot record, int[] targetIos) {
        this.record = record;
        this.targetIos = targetIos;
    }

    private boolean isTargetIo(int ioType) {
        for (int targetIo : targetIos) {
            if (targetIo == ioType)
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        byte[] recordBytes = U.field(record, "pageDataBytes");
        int realPageSize = U.field(record, "realPageSize");

        ByteBuffer buf = ByteBuffer.allocateDirect(realPageSize);
        buf.order(ByteOrder.nativeOrder());
        buf.put(recordBytes);

        long addr = GridUnsafe.bufferAddress(buf);

        try {
            PageIO io = getPageIO(addr);

            if (!isTargetIo(io.getType())) {
                return record.toString();
            }

            String repr = record.toString();

            return repr.replaceFirst("page = \\[", "page = [\nbytes=0x" + byteArray2HexString(recordBytes)) + ",";
        }
        catch (Exception e) {
            return record + " IO_UNPARSED";
        }
        finally {
            GridUnsafe.cleanDirectBuffer(buf);
        }
    }
}
