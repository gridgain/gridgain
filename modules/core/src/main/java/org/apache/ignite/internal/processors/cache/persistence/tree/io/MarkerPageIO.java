/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Marker page.
 */
public class MarkerPageIO extends PageIO {
    /** Offset for marker type value. */
    private static final int MARKER_TYPE_OFF = COMMON_HEADER_END;

    /**
     * Offset for WAL record serializer version, see {@link #walRecordSerializerVersion(long)}.
     */
    private static final int WAL_RECORD_SERIALIZER_VERSION_OFF = MARKER_TYPE_OFF + 4;

    /**
     * Offset for count for WAL records that follow this marker.
     */
    public static final int WAL_RECORDS_CNT_OFF = WAL_RECORD_SERIALIZER_VERSION_OFF + 4;

    /** This type of marker is used to mark the end of pages stream. */
    public static final int MARKER_TYPE_TERMINATION = 1;

    /** */
    public static final IOVersions<MarkerPageIO> VERSIONS = new IOVersions<>(
        new MarkerPageIO(1)
    );

    /**
     * @param ver Page format version.
     */
    public MarkerPageIO(int ver) {
        super(T_MARKER_PAGE, ver);
    }

    /**
     * @param type Page type.
     * @param ver  Page format version.
     */
    protected MarkerPageIO(int type, int ver) {
        super(type, ver);
    }

    /**
     * Type of a marker.
     *
     * @param pageAddr Page address.
     */
    public int markerType(long pageAddr) {
        return PageUtils.getInt(pageAddr, MARKER_TYPE_OFF);
    }

    /**
     * Type of a marker.
     *
     * @param buf Page buffer.
     */
    public int markerType(ByteBuffer buf) {
        return buf.getInt(MARKER_TYPE_OFF);
    }

    /**
     * Sets marker type.
     *
     * @param pageAddr Page address.
     * @param markerType Marker type.
     */
    public void setMarkerType(long pageAddr, int markerType) {
        assertPageType(pageAddr);

        PageUtils.putInt(pageAddr, MARKER_TYPE_OFF, markerType);
    }

    /**
     * Pages stream can be followed by serialized WAL records, version of record serializer is stored in marker page.
     * This method returns WAL record serializer version.
     *
     * @param pageAddr Page address.
     */
    public int walRecordSerializerVersion(long pageAddr) {
        return PageUtils.getInt(pageAddr, WAL_RECORD_SERIALIZER_VERSION_OFF);
    }

    /**
     * Returns WAL record serializer version, see {@link #walRecordSerializerVersion(long)}.
     *
     * @param buf Page buffer.
     */
    public int walRecordSerializerVersion(ByteBuffer buf) {
        return buf.getInt(WAL_RECORD_SERIALIZER_VERSION_OFF);
    }

    /**
     * Sets WAL record serializer version, see {@link #walRecordSerializerVersion(long)}.
     *
     * @param pageAddr Page address.
     * @param v Version.
     */
    public void setWalRecordSerializerVersion(long pageAddr, int v) {
        assertPageType(pageAddr);

        PageUtils.putInt(pageAddr, WAL_RECORD_SERIALIZER_VERSION_OFF, v);
    }

    /**
     * Returns WAL records count.
     *
     * @param pageAddr Page address.
     */
    public int walRecordsCnt(long pageAddr) {
        return PageUtils.getInt(pageAddr, WAL_RECORDS_CNT_OFF);
    }

    /**
     * Returns WAL records count.
     *
     * @param buf Page buffer.
     */
    public int walRecordsCnt(ByteBuffer buf) {
        return buf.getInt(WAL_RECORDS_CNT_OFF);
    }

    /**
     * Sets WAL records count.
     *
     * @param pageAddr Page address.
     * @param v WAL records count.
     */
    public void setWalRecordsCnt(long pageAddr, int v) {
        assertPageType(pageAddr);

        PageUtils.putInt(pageAddr, WAL_RECORDS_CNT_OFF, v);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("MarkerPage [markerType=" + markerType(addr) +
            ", walRecordSerializerVersion=" + walRecordSerializerVersion(addr) + "]");
    }
}
