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

package org.apache.ignite.ml.inference.storage.model;

import java.util.function.Supplier;

/**
 * Model storage provider that keeps files and directories presented as {@link FileOrDirectory} files and correspondent
 * locks.
 */
public interface ModelStorageProvider {
    /**
     * Returns file or directory associated with the specified path. Will acquire an internal storage provider lock
     * on the {@code path} when called inside {@link #synchronize(Supplier)} invocation.
     *
     * @param path Path of a file or a directory.
     * @return File or directory associated with the specified path, {@code null} if no file or directory
     * is associated with the path.
     */
    public FileOrDirectory get(String path);

    /**
     * Saves file or directory associated with the specified path. Will acquire an internal storage provider lock
     * on the {@code path} when called inside {@link #synchronize(Supplier)} invocation.
     *
     * @param path Path to the file or directory.
     * @param file File or directory to be saved.
     */
    public void put(String path, FileOrDirectory file);

    /**
     * Removes file or directory associated with the specified path. Will acquire an internal storage provider lock
     * on the {@code path} when called inside {@link #synchronize(Supplier)} invocation.
     *
     * @param path Path to the file or directory.
     */
    public void remove(String path);

    /**
     * Synchronizes accesses to this storage provider inside the given invocation. Calls to {@link #get(String)},
     * {@link #put(String, FileOrDirectory)}, {@link #remove(String)} will automatically acquire a lock on the
     * given key to synchronize actions across multiple threads. The locks will be released upon closure exit.
     *
     * @param invocation {@code Runnable} to execute.
     */
    public <T> T synchronize(Supplier<T> invocation);
}
