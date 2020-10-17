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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;

/**
 * Utility to read and write {@link GridCacheVersion} instances.
 */
public class CacheVersionIO {
    /** */
    private static final byte NULL_PROTO_VER = 0;

    /** */
    private static final byte MIN_PROTO_VER = -2;

    /** */
    private static final byte MAX_PROTO_VER = 2;

    /** */
    private static final int NULL_SIZE = 1;

    /** Serialized size in bytes. */
    private static final int SIZE_V1 = 17;

    /** Serialized size in bytes. */
    private static final int SIZE_V2 = 33;

    /** Serialized size in bytes. */
    private static final int SIZE_GG_V1 = 25;

    /** Serialized size in bytes. */
    private static final int SIZE_GG_V2 = 41;

    /**
     * @param ver Version.
     * @param allowNull Is {@code null} version allowed.
     * @return Serialized size in bytes.
     */
    public static int size(GridCacheVersion ver, boolean allowNull) {
        if (ver == null) {
            if (allowNull)
                return NULL_SIZE;

            throw new IllegalStateException("Cache version is null");
        }

        if (ver.updateCounter() == 0)
            return ver instanceof GridCacheVersionEx ? SIZE_V2 : SIZE_V1;
        else
            return ver instanceof GridCacheVersionEx ? SIZE_GG_V2 : SIZE_GG_V1;
    }

    /**
     * @param buf Byte buffer.
     * @param ver Version to write.
     * @param allowNull Is {@code null} version allowed.
     */
    public static void write(ByteBuffer buf, GridCacheVersion ver, boolean allowNull) {
        if (ver == null) {
            if (allowNull)
                buf.put(NULL_PROTO_VER);
            else
                throw new IllegalStateException("Cache version is null");
        }
        else if (ver.updateCounter() != 0) {
            if (ver instanceof GridCacheVersionEx) {
                byte protoVer = -2; // Version of serialization protocol.

                buf.put(protoVer);
                buf.putInt(ver.topologyVersion());
                buf.putInt(ver.nodeOrderAndDrIdRaw());
                buf.putLong(ver.order());
                buf.putLong(ver.updateCounter());

                buf.putInt(ver.conflictVersion().topologyVersion());
                buf.putInt(ver.conflictVersion().nodeOrderAndDrIdRaw());
                buf.putLong(ver.conflictVersion().order());
            }
            else {
                byte protoVer = -1; // Version of serialization protocol.

                buf.put(protoVer);
                buf.putInt(ver.topologyVersion());
                buf.putInt(ver.nodeOrderAndDrIdRaw());
                buf.putLong(ver.order());
                buf.putLong(ver.updateCounter());
            }
        }
        else {
            if (ver instanceof GridCacheVersionEx) {
                byte protoVer = 2; // Version of serialization protocol.

                buf.put(protoVer);
                buf.putInt(ver.topologyVersion());
                buf.putInt(ver.nodeOrderAndDrIdRaw());
                buf.putLong(ver.order());

                buf.putInt(ver.conflictVersion().topologyVersion());
                buf.putInt(ver.conflictVersion().nodeOrderAndDrIdRaw());
                buf.putLong(ver.conflictVersion().order());
            }
            else {
                byte protoVer = 1; // Version of serialization protocol.

                buf.put(protoVer);
                buf.putInt(ver.topologyVersion());
                buf.putInt(ver.nodeOrderAndDrIdRaw());
                buf.putLong(ver.order());
            }
        }
    }

    /**
     * @param addr Write address.
     * @param ver Version to write.
     * @param allowNull Is {@code null} version allowed.
     */
    public static void write(long addr, GridCacheVersion ver, boolean allowNull) {
        if (ver == null) {
            if (allowNull)
                PageUtils.putByte(addr, 0, NULL_PROTO_VER);
            else
                throw new IllegalStateException("Cache version is null");
        }
        else if (ver.updateCounter() != 0) {
            if (ver instanceof GridCacheVersionEx) {
                byte protoVer = -2; // Version of serialization protocol.

                PageUtils.putByte(addr, 0, protoVer);
                PageUtils.putInt(addr, 1, ver.topologyVersion());
                PageUtils.putInt(addr, 5, ver.nodeOrderAndDrIdRaw());
                PageUtils.putLong(addr, 9, ver.order());
                PageUtils.putLong(addr, 17, ver.updateCounter());

                PageUtils.putInt(addr, 25, ver.conflictVersion().topologyVersion());
                PageUtils.putInt(addr, 29, ver.conflictVersion().nodeOrderAndDrIdRaw());
                PageUtils.putLong(addr, 33, ver.conflictVersion().order());
            }
            else {
                byte protoVer = -1; // Version of serialization protocol.

                PageUtils.putByte(addr, 0, protoVer);
                PageUtils.putInt(addr, 1, ver.topologyVersion());
                PageUtils.putInt(addr, 5, ver.nodeOrderAndDrIdRaw());
                PageUtils.putLong(addr, 9, ver.order());
                PageUtils.putLong(addr, 17, ver.updateCounter());
            }
        }
        else {
            if (ver instanceof GridCacheVersionEx) {
                byte protoVer = 2; // Version of serialization protocol.

                PageUtils.putByte(addr, 0, protoVer);
                PageUtils.putInt(addr, 1, ver.topologyVersion());
                PageUtils.putInt(addr, 5, ver.nodeOrderAndDrIdRaw());
                PageUtils.putLong(addr, 9, ver.order());

                PageUtils.putInt(addr, 17, ver.conflictVersion().topologyVersion());
                PageUtils.putInt(addr, 21, ver.conflictVersion().nodeOrderAndDrIdRaw());
                PageUtils.putLong(addr, 25, ver.conflictVersion().order());
            }
            else {
                byte protoVer = 1; // Version of serialization protocol.

                PageUtils.putByte(addr, 0, protoVer);
                PageUtils.putInt(addr, 1, ver.topologyVersion());
                PageUtils.putInt(addr, 5, ver.nodeOrderAndDrIdRaw());
                PageUtils.putLong(addr, 9, ver.order());
            }
        }
    }

    /**
     * @param protoVer Serialization protocol version.
     * @param allowNull Is {@code null} version allowed.
     * @return Protocol version.
     * @throws IgniteCheckedException if failed.
     */
    private static byte checkProtocolVersion(byte protoVer, boolean allowNull) throws IgniteCheckedException {
        if (protoVer >= MIN_PROTO_VER && protoVer <= MAX_PROTO_VER) {
            if (protoVer == NULL_PROTO_VER && !allowNull)
                throw new IllegalStateException("Cache version is null.");

            return protoVer;
        }

        throw new IgniteCheckedException("Unsupported protocol version: " + protoVer);
    }

    /**
     * Gets needed buffer size to read the whole version instance.
     * Does not change buffer position.
     *
     * @param buf Buffer.
     * @param allowNull Is {@code null} version allowed.
     * @return Size of serialized version.
     * @throws IgniteCheckedException If failed.
     */
    public static int readSize(ByteBuffer buf, boolean allowNull) throws IgniteCheckedException {
        byte protoVer = checkProtocolVersion(buf.get(buf.position()), allowNull);

        return sizeForVersion(protoVer);
    }

    /**
     * Gets needed buffer size to read the whole version instance.
     * Does not change buffer position.
     *
     * @param pageAddr Page address.
     * @param allowNull Is {@code null} version allowed.
     * @return Size of serialized version.
     * @throws IgniteCheckedException If failed.
     */
    public static int readSize(long pageAddr, boolean allowNull) throws IgniteCheckedException {
        byte protoVer = checkProtocolVersion(PageUtils.getByte(pageAddr, 0), allowNull);

        return sizeForVersion(protoVer);
    }

    /**
     * Get size for protocol version.
     *
     * @param protoVer Protocol version.
     * @return Size.
     */
    private static int sizeForVersion(int protoVer) {
        switch (protoVer) {
            case NULL_PROTO_VER:
                return NULL_SIZE;

            case 1:
                return SIZE_V1;

            case 2:
                return SIZE_V2;

            case -1:
                return SIZE_GG_V1;

            case -2:
                return SIZE_GG_V2;

            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Reads GridCacheVersion instance from the given buffer. Moves buffer's position by the number of used
     * bytes.
     *
     * @param buf Byte buffer.
     * @param allowNull Is {@code null} version allowed.
     * @return Version.
     * @throws IgniteCheckedException If failed.
     */
    public static GridCacheVersion read(ByteBuffer buf, boolean allowNull) throws IgniteCheckedException {
        byte protoVer = checkProtocolVersion(buf.get(), allowNull);

        switch (protoVer) {
            case NULL_PROTO_VER:
                return null;

            case 1: {
                int topVer = buf.getInt();
                int nodeOrderDrId = buf.getInt();
                long order = buf.getLong();

                return new GridCacheVersion(topVer, nodeOrderDrId, order);
            }

            case 2: {
                int topVer = buf.getInt();
                int nodeOrderDrId = buf.getInt();
                long order = buf.getLong();

                int conflictTop = buf.getInt();
                int conflictNodeOrderDrId = buf.getInt();
                long conflictOrder = buf.getLong();

                return new GridCacheVersionEx(topVer, nodeOrderDrId, order,
                    new GridCacheVersion(conflictTop, conflictNodeOrderDrId, conflictOrder));
            }

            case -1: {
                int topVer = buf.getInt();
                int nodeOrderDrId = buf.getInt();
                long order = buf.getLong();
                long updCntr = buf.getLong();

                GridCacheVersion res = new GridCacheVersion(topVer, nodeOrderDrId, order);

                res.updateCounter(updCntr);

                return res;
            }

            case -2: {
                int topVer = buf.getInt();
                int nodeOrderDrId = buf.getInt();
                long order = buf.getLong();
                long updCntr = buf.getLong();

                int conflictTop = buf.getInt();
                int conflictNodeOrderDrId = buf.getInt();
                long conflictOrder = buf.getLong();

                GridCacheVersionEx res = new GridCacheVersionEx(topVer, nodeOrderDrId, order,
                        new GridCacheVersion(conflictTop, conflictNodeOrderDrId, conflictOrder));

                res.updateCounter(updCntr);

                return res;
            }

            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Reads GridCacheVersion instance from the given address.
     *
     * @param pageAddr Page address.
     * @param allowNull Is {@code null} version allowed.
     * @return Version.
     * @throws IgniteCheckedException If failed.
     */
    public static GridCacheVersion read(long pageAddr, boolean allowNull) throws IgniteCheckedException {
        byte protoVer = checkProtocolVersion(PageUtils.getByte(pageAddr, 0), allowNull);

        switch (protoVer) {
            case NULL_PROTO_VER:
                return null;

            case 1: {
                int topVer = PageUtils.getInt(pageAddr, 1);
                int nodeOrderDrId = PageUtils.getInt(pageAddr, 5);
                long order = PageUtils.getLong(pageAddr, 9);

                return new GridCacheVersion(topVer, nodeOrderDrId, order);
            }

            case 2: {
                int topVer = PageUtils.getInt(pageAddr, 1);
                int nodeOrderDrId = PageUtils.getInt(pageAddr, 5);
                long order = PageUtils.getLong(pageAddr, 9);

                int conflictTop = PageUtils.getInt(pageAddr, 17);
                int conflictNodeOrderDrId = PageUtils.getInt(pageAddr, 21);
                long conflictOrder = PageUtils.getLong(pageAddr, 25);

                return new GridCacheVersionEx(topVer, nodeOrderDrId, order,
                    new GridCacheVersion(conflictTop, conflictNodeOrderDrId, conflictOrder));
            }

            case -1: {
                int topVer = PageUtils.getInt(pageAddr, 1);
                int nodeOrderDrId = PageUtils.getInt(pageAddr, 5);
                long order = PageUtils.getLong(pageAddr, 9);
                long updCntr = PageUtils.getLong(pageAddr, 17);

                GridCacheVersion res = new GridCacheVersion(topVer, nodeOrderDrId, order);

                res.updateCounter(updCntr);

                return res;
            }

            case -2: {
                int topVer = PageUtils.getInt(pageAddr, 1);
                int nodeOrderDrId = PageUtils.getInt(pageAddr, 5);
                long order = PageUtils.getLong(pageAddr, 9);
                long updCntr = PageUtils.getLong(pageAddr, 17);

                int conflictTop = PageUtils.getInt(pageAddr, 25);
                int conflictNodeOrderDrId = PageUtils.getInt(pageAddr, 29);
                long conflictOrder = PageUtils.getLong(pageAddr, 33);

                GridCacheVersionEx res = new GridCacheVersionEx(topVer, nodeOrderDrId, order,
                        new GridCacheVersion(conflictTop, conflictNodeOrderDrId, conflictOrder));

                res.updateCounter(updCntr);

                return res;
            }

            default:
                throw new IllegalStateException();
        }
    }
}
