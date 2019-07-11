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

import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * Default implementation of {@link ModelStorage} that can use any {@link ModelStorageProvider} as a backend storage
 * system.
 */
public class DefaultModelStorage implements ModelStorage {
    /** Ignite Cache that is used to store model storage files. */
    private final ModelStorageProvider storageProvider;

    /**
     * Constructs a new instance of Ignite model storage.
     *
     * @param storageProvider Model storage provider.
     */
    public DefaultModelStorage(ModelStorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    /** {@inheritDoc} */
    @Override public void putFile(String path, byte[] data, boolean onlyIfNotExist) {
        String parentPath = getParent(path);

        storageProvider.synchronize(() -> {
            FileOrDirectory pathFileOrDir = storageProvider.get(path);

            // Paths are locked in child-first order (exists will acquire the path lock).
            if (pathFileOrDir != null && onlyIfNotExist)
                throw new IllegalArgumentException("File already exists [path=" + path + "]");

            FileOrDirectory parent = storageProvider.get(parentPath);

            // If parent doesn't exist throw an exception.
            if (parent == null)
                throw new IllegalArgumentException("Cannot create file because directory doesn't exist [path="
                    + path + "]");

            // If parent is not a directory throw an exception.
            if (!parent.isDirectory())
                throw new IllegalArgumentException("Cannot create file because parent is not a directory [path="
                    + path + "]");

            Directory dir = (Directory)parent;
            // Update parent if it's a new file.
            if (!dir.getFiles().contains(path)) {
                dir.getFiles().add(path);
                storageProvider.put(parentPath, dir.updateModifictaionTs());
            }

            // Save file into cache.
            storageProvider.put(path, new File(data));

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public byte[] getFile(String path) {
        return storageProvider.synchronize(() -> {
            FileOrDirectory fileOrDir = storageProvider.get(path);

            // If file doesn't exist throw an exception.
            if (fileOrDir == null)
                throw new IllegalArgumentException("File doesn't exist [path=" + path + "]");

            // If file is not a regular file throw an exception.
            if (!fileOrDir.isFile())
                throw new IllegalArgumentException("File is not a regular file [path=" + path + "]");

            return ((File)fileOrDir).getData();
        });
    }

    /** {@inheritDoc} */
    @Override public void mkdir(String path, boolean onlyIfNotExist) {
        String parentPath = getParent(path);

        storageProvider.synchronize(() -> {
            // Paths are locked in child-first order.
            FileOrDirectory pathFileOrDir = storageProvider.get(path);

            // If a directory associated with specified path exists return.
            if (pathFileOrDir != null && pathFileOrDir.isDirectory()) {
                if (onlyIfNotExist)
                    throw new IllegalArgumentException("Directory already exists [path=" + path + "]");

                return null;
            }

            // If a regular file associated with specified path exists throw an exception.
            if (pathFileOrDir != null && pathFileOrDir.isFile())
                throw new IllegalArgumentException("File with specified path already exists [path=" + path + "]");

            FileOrDirectory parent = storageProvider.get(parentPath);

            // If parent doesn't exist throw an exception.
            if (parent == null)
                throw new IllegalArgumentException("Cannot create directory because parent directory does not exist"
                    + " [path=" + path + "]");

            // If parent is not a directory throw an exception.
            if (!parent.isDirectory())
                throw new IllegalArgumentException("Cannot create directory because parent is not a directory [path="
                    + path + "]");

            Directory dir = (Directory)parent;
            dir.getFiles().add(path);

            // Update parent and save directory into cache.
            storageProvider.put(parentPath, parent.updateModifictaionTs());
            storageProvider.put(path, new Directory());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(String path0) {
        storageProvider.synchronize(() -> {
            String path = path0;
            Deque<String> pathsToBeCreated = new LinkedList<>();

            String parent = null;

            while (path != null) {
                // Paths are locked in child-first order.
                storageProvider.get(path);

                pathsToBeCreated.push(path);

                if (exists(path)) {
                    if (isDirectory(path)) {
                        parent = pathsToBeCreated.pop();

                        break;
                    }

                    throw new IllegalArgumentException("Cannot create directory because parent is not a directory "
                        + "[path=" + path + "]");
                }

                path = getParent(path);
            }

            while (!pathsToBeCreated.isEmpty()) {
                String pathToCreate = pathsToBeCreated.pop();

                storageProvider.put(pathToCreate, new Directory());

                if (parent != null) {
                    Directory parentDir = (Directory)storageProvider.get(parent);

                    parentDir.getFiles().add(pathToCreate);

                    storageProvider.put(parent, parentDir.updateModifictaionTs());
                }

                parent = pathToCreate;
            }

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public Set<String> listFiles(String path) {
        return storageProvider.synchronize(() -> {
            FileOrDirectory dir = storageProvider.get(path);

            // If directory doesn't exist throw an exception.
            if (dir == null)
                throw new IllegalArgumentException("Directory doesn't exist [path=" + path + "]");

            // If directory isn't a directory throw an exception.
            if (!dir.isDirectory())
                throw new IllegalArgumentException("Specified path is not associated with directory [path=" + path
                    + "]");

            return ((Directory)dir).getFiles();
        });
    }

    /** {@inheritDoc} */
    @Override public void remove(String path) {
        String parentPath = getParent(path);

        storageProvider.synchronize(() -> {
            FileOrDirectory file = storageProvider.get(path);

            if (file.isDirectory()) {
                Directory dir = (Directory) file;

                if (!dir.getFiles().isEmpty())
                    throw new IllegalArgumentException("Cannot delete non-empty directory [path=" + path + "]");
            }

            storageProvider.remove(path);

            Directory parent = (Directory)storageProvider.get(parentPath);

            if (parent != null) {
                parent.getFiles().remove(path);

                storageProvider.put(parentPath, parent);
            }

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public boolean exists(String path) {
        return storageProvider.get(path) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectory(String path) {
        FileOrDirectory file = storageProvider.get(path);

        return file != null && file.isDirectory();
    }

    /** {@inheritDoc} */
    @Override public boolean isFile(String path) {
        FileOrDirectory file = storageProvider.get(path);

        return file != null && file.isFile();
    }

    /** {@inheritDoc} */
    @Override public FileStat getFileStat(String path) {
        FileOrDirectory file = storageProvider.get(path);
        if (file == null)
            throw new IllegalArgumentException("File not found [path=" + path + "]");

        int size = 0;
        if (file.isFile())
            size = ((File)file).getData().length;
        return new FileStat(file.isDirectory(), file.getModificationTs(), size);
    }

    /** {@inheritDoc} */
    @Override public <T> T lockPaths(Supplier<T> supplier, String... paths) {
        return storageProvider.synchronize(() -> {
            for (String path : paths)
                storageProvider.get(path);

            return supplier.get();
        });
    }

    /**
     * Returns parent directory for the specified path.
     *
     * @param path Path.
     * @return Parent directory path.
     */
    private String getParent(String path) {
        String[] splittedPath = path.split("/");

        int cnt = 0;

        for (String s : splittedPath) {
            if (!s.isEmpty())
                cnt++;
        }

        if (cnt == 0)
            return null;

        StringBuilder parentPath = new StringBuilder("/");

        for (String s : splittedPath) {
            if (!s.isEmpty() && --cnt > 0)
                parentPath.append(s).append("/");
        }

        if (parentPath.length() > 1)
            parentPath.delete(parentPath.length() - 1, parentPath.length());

        return parentPath.toString();
    }

    /**
     * Wraps task execution into locks.
     *
     * @param task Runnable task.
     * @param locks List of locks.
     */
    static void synchronize(Runnable task, Lock... locks) {
        synchronize(() -> {
            task.run();
            return null;
        }, locks);
    }

    /**
     * Wraps task execution into locks. Util method.
     *
     * @param task Task to executed.
     * @param locks List of locks.
     */
    static <T> T synchronize(Supplier<T> task, Lock... locks) {
        Throwable ex = null;
        T res;

        int i = 0;
        try {
            for (; i < locks.length; i++)
                locks[i].lock();

            res = task.get();
        }
        finally {
            for (i = i - 1; i >= 0; i--) {
                try {
                    locks[i].unlock();
                }
                catch (RuntimeException | Error e) {
                    ex = e;
                }
            }
        }

        if (ex != null) {
            if (ex instanceof RuntimeException)
                throw (RuntimeException)ex;

            if (ex instanceof Error)
                throw (Error)ex;

            throw new IllegalStateException("Unexpected type of throwable");
        }

        return res;
    }
}
