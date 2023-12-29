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
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointPagesWriter.CheckpointPageStoreInfo;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Class that holds utilities for "syncfs" function that can be used during checkpoint.
 */
public class SyncFsUtils {
    /**
     * Name of the system property that enables {@code syncfs} checkpoint optimization.
     */
    public static final String IGNITE_CHECKPOINT_SYNC_FS_ENABLED = "IGNITE_CHECKPOINT_SYNC_FS_ENABLED";

    /**
     * Name of the system property that controls the threshold for the number of files that will trigger the
     * {@code syncfs} instead of multiple {@code fsync} calls for each individual file in the checkpoint.
     */
    public static final String IGNITE_CHECKPOINT_SYNC_FS_THRESHOLD = "IGNITE_CHECKPOINT_SYNC_FS_THRESHOLD";

    /**
     * Value of the {@link #IGNITE_CHECKPOINT_SYNC_FS_ENABLED} system property. {@code true} by default.
     */
    public static final boolean SYNC_FS_ENABLED = getBoolean(IGNITE_CHECKPOINT_SYNC_FS_ENABLED, true);

    /**
     * Value of the {@link #IGNITE_CHECKPOINT_SYNC_FS_THRESHOLD} system property. {@code 1024} by default.
     */
    public static final int SYNC_FS_THRESHOLD = getInteger(IGNITE_CHECKPOINT_SYNC_FS_THRESHOLD, 1024);

    /**
     * Executes {@code syncfs} on all filesystems that contain files from the given set of page stores.
     * According to {@code man}:
     * <p/><i>syncfs() is like sync(), but synchronizes just the filesystem containing file referred to by the open file
     * descriptor fd.<br/>
     * ... sync() or syncfs() provide the same guarantees as fsync() called on every file in the system or
     * filesystem respectively.</i>
     *
     * @param updStores Internal map from the checkpointer that contains all updated page stores.
     * @throws IgniteCheckedException If operation failed.
     */
    public static void syncfs(
        ConcurrentMap<PageStore, CheckpointPageStoreInfo> updStores
    ) throws IgniteCheckedException {
        Collection<Path> filesFromDifferentFileSystems = findAllFileSystems(updStores);

        // Resulting collection should not be big. For most installations it will only consist of a single element.
        syncFileSystems(filesFromDifferentFileSystems);
    }

    private static Collection<Path> findAllFileSystems(
        ConcurrentMap<PageStore, CheckpointPageStoreInfo> updStores
    ) throws StorageException {
        IntHashMap<IgniteBiTuple<Path, FileStore>> groupIdToFileStore = new IntHashMap<>();

        // First pass is to split all file stores by the groupId. All partitions within a group are always in the same
        // directory.
        for (Map.Entry<PageStore, CheckpointPageStoreInfo> entry : updStores.entrySet()) {
            CheckpointPageStoreInfo info = entry.getValue();

            if (groupIdToFileStore.containsKey(info.groupId))
                continue;

            Path path = ((FilePageStore) entry.getKey()).getPath();

            try {
                groupIdToFileStore.put(
                    info.groupId,
                    new IgniteBiTuple<>(path, Files.getFileStore(path))
                );
            }
            catch (IOException e) {
                throw new StorageException(e);
            }
        }

        // Maps file store name to a path from that file store. Second pass is to fill this map.
        Map<String, Path> fileSystems = new HashMap<>();

        groupIdToFileStore.forEach((key, val) -> fileSystems.putIfAbsent(val.getValue().name(), val.getKey()));

        return fileSystems.values();
    }

    @SuppressWarnings("ThrowFromFinallyBlock")
    private static void syncFileSystems(Collection<Path> filesFromDifferentFileSystems) throws IgniteCheckedException {
        for (Path path : filesFromDifferentFileSystems) {
            // Open with default flags and mode in order to get file descriptor.
            int fd = SyncFsLibC.open(path.toAbsolutePath().toString(), 0, 0);

            int syncFsErr = 0;

            Throwable error = null;

            try {
                if (SyncFsLibC.syncfs(fd) != 0)
                    syncFsErr = Native.getLastError();
            }
            catch (RuntimeException | Error e) {
                error = e;
            }
            finally {
                int closeErr = 0;

                if (SyncFsLibC.close(fd) != 0)
                    closeErr = Native.getLastError();

                IgniteCheckedException igniteException = null;

                if (closeErr != 0 || syncFsErr != 0) {
                    List<String> messages = new ArrayList<>(2);

                    if (syncFsErr != 0)
                        messages.add(SyncFsLibC.strerror(syncFsErr));

                    if (closeErr != 0)
                        messages.add(SyncFsLibC.strerror(closeErr));

                    igniteException = new IgniteCheckedException("Failed to execute syncfs: " + messages);
                }

                if (error != null) {
                    if (igniteException != null)
                        error.addSuppressed(igniteException);

                    if (error instanceof RuntimeException)
                        throw (RuntimeException)error;
                    else
                        throw (Error)error;
                }
                else if (igniteException != null)
                    throw igniteException;
            }
        }
    }

    /**
     * Returns {@code true} if {@code IGNITE_CHECKPOINT_SYNC_FS_ENABLED} system property is set to {@code true} and
     * current operating system supports {@code syncfs} function.
     */
    public static boolean isActive() {
        return SYNC_FS_ENABLED && SyncFsLibC.JNA_AVAILABLE;
    }

    /**
     * Logs the startup message if necessary.
     *
     * @param log Logger.
     */
    public static void logStartup(IgniteLogger log) {
        if (SyncFsUtils.SYNC_FS_ENABLED) {
            if (SyncFsUtils.isActive())
                log.info("syncfs is enabled with a threshold of " + SyncFsUtils.SYNC_FS_THRESHOLD + " files.");
            else
                log.warning("syncfs checkpoint optimization is enabled, but not supported by the OS.");

            if (SyncFsLibC.ERROR != null)
                log.error("Exception while loading native library for syncfs", SyncFsLibC.ERROR);
        }
    }
}
