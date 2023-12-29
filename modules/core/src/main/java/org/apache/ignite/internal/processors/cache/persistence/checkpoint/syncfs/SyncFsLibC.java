/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint.syncfs;

import com.sun.jna.Native;
import com.sun.jna.Platform;

/**
 * Native IO library.
 */
class SyncFsLibC {
    /** JNA library available and initialized. {@code true} for linux with version >= 2.14. */
    public static final boolean JNA_AVAILABLE;

    static {
        boolean jnaAvailable0 = false;

        if (Platform.isLinux()) {
            try {
                if (checkLinuxVersion()) {
                    Native.register(Platform.C_LIBRARY_NAME);
                    jnaAvailable0 = true;
                }
            }
            catch (Throwable ignore) {
            }
        }

        JNA_AVAILABLE = jnaAvailable0;
    }

    private static boolean checkLinuxVersion() {
        final String osVer = System.getProperty("os.version");

        if (osVer == null)
            return false;

        String[] split = osVer.split("[.-]");
        int ver = Integer.parseInt(split[0]);
        int major = Integer.parseInt(split[1]);

        return ver > 2 || ver == 2 && major >= 14;
    }

    /**
     * Opens a file.
     *
     * @param pathname Path to the file.
     * @param flags Open options.
     * @param mode Create file mode.
     * @return File descriptor.
     */
    public static native int open(String pathname, int flags, int mode);

    /**
     * Closes a file.
     *
     * @param fd File descriptor.
     * @return {@code 0} if successful, {@code -1} if failed.
     */
    public static native int close(int fd);

    /**
     * Calls a {@code syncfs}, synchronizing all files from the same file system as the file with given descriptor.
     *
     * @param fd File descriptor.
     * @return {@code 0} if successful, {@code -1} if failed.
     */
    public static native int syncfs(int fd);

    /**
     * Returns a string that describes the error code.
     *
     * @param errno Error code.
     * @return Displayable error information.
     */
    public static native String strerror(int errno);
}
