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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A memory-resident {@link Directory} implementation.
 */
public class GridLuceneDirectory extends BaseDirectory implements Accountable {

    /**
     * Constant for efficient list -> array transformation
     */
    public static final String[] STRINGS = new String[0];

    /** */
    protected final Map<String, GridLuceneFile> fileMap = new ConcurrentHashMap<>();

    /** */
    protected final Set<String> pendingDeletions = new GridConcurrentHashSet<>();

    /** */
    protected final AtomicLong sizeInBytes = new AtomicLong();

    /** */
    protected final AtomicInteger nextTmpFileIndex = new AtomicInteger(0);

    /** */
    private final GridUnsafeMemory mem;

    /**
     * Constructs an empty {@link Directory}.
     *
     * @param mem Memory.
     */
    public GridLuceneDirectory(GridUnsafeMemory mem) {
        super(new GridLuceneLockFactory());

        this.mem = mem;
    }

    /** {@inheritDoc} */
    @Override public final String[] listAll() {
        ensureOpen();
        // NOTE: fileMap.keySet().toArray(new String[0]) is broken in non Sun JDKs,
        // and the code below is resilient to map changes during the array population.
        Set<String> fileNames = fileMap.keySet();

        List<String> names = new ArrayList<>(fileNames);

        return names.toArray(STRINGS);
    }

    /** {@inheritDoc} */
    @Override public void rename(String source, String dest) throws IOException {
        ensureOpen();

        GridLuceneFile file = fileMap.get(source);

        if (file == null)
            throw new FileNotFoundException(source);

        fileMap.put(dest, file);

        fileMap.remove(source);
    }

    /** {@inheritDoc} */
    @Override public void syncMetaData() throws IOException {
        // Noop. No meta data sync needed as all data is in-memory.
    }

    /** {@inheritDoc} */
    @Override public IndexOutput createTempOutput(String prefix, String suffix, IOContext ctx) throws IOException {
        String suffixWithIndex = suffix + "_" + Long.toString(nextTmpFileIndex.getAndIncrement(), Character.MAX_RADIX);
        String name = IndexFileNames.segmentFileName(prefix, suffixWithIndex, "tmp");

        return createOutput(name, ctx);
    }

    /** {@inheritDoc} */
    @Override public final long fileLength(String name) throws IOException {
        ensureOpen();

        GridLuceneFile file = fileMap.get(name);

        if (file == null)
            throw new FileNotFoundException(name);

        return file.getLength();
    }

    /** {@inheritDoc} */
    @Override public void deleteFile(String name) throws IOException {
        ensureOpen();

        doDeleteFile(name, false);
    }

    /**
     * Deletes file.
     *
     * @param name File name.
     * @param onClose If on close directory;
     * @throws IOException If failed.
     */
    private void doDeleteFile(String name, boolean onClose) throws IOException {
        GridLuceneFile file = fileMap.remove(name);

        if (file != null) {
            doDeleteFile0(name, file);

            // All files should be closed when Directory is closing.
            assert !onClose || !file.hasRefs() : "Possible memory leak, resource is not closed: " + file.toString();

            sizeInBytes.addAndGet(-file.getSizeInBytes());
        }
        else
            throw new FileNotFoundException(name);
    }

    /**
     * Call actual delete operation and add filename to pending deletions set
     *
     * @param name      File name.
     * @param file      File instance.
     * @throws IOException If failed
     *
     * @see GridLuceneFile#deferredDelete()
     */
    private void doDeleteFile0(String name, GridLuceneFile file) throws IOException {
        // Filename would be removed from pending deletions when
        // GridLuceneFile.deferredDelete() will finish his job
        pendingDeletions.add(name);
        file.delete();
    }

    /** {@inheritDoc} */
    @Override public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        ensureOpen();

        GridLuceneFile file = new GridLuceneFile(this, name);

        // Lock for using in stream. Will be unlocked on stream closing.
        file.lockRef();

        GridLuceneFile existing = fileMap.put(name, file);

        if (existing != null) {
            sizeInBytes.addAndGet(-existing.getSizeInBytes());

            doDeleteFile0(name, existing);
        }

        return new GridLuceneOutputStream(file);
    }

    /** {@inheritDoc} */
    @Override public void sync(final Collection<String> names) throws IOException {
        // Noop. No fsync needed as all data is in-memory.
    }

    /** {@inheritDoc} */
    @Override public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();

        GridLuceneFile file = fileMap.get(name);

        if (file == null)
            throw new FileNotFoundException(name);

        // Lock for using in stream. Will be unlocked on stream closing.
        file.lockRef();

        if (!fileMap.containsKey(name)) {
            // Unblock for deferred delete.
            file.releaseRef();

            throw new FileNotFoundException(name);
        }

        return new GridLuceneInputStream(name, file);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        isOpen = false;

        IgniteException errs = null;

        for (String fileName : fileMap.keySet()) {
            try {
                doDeleteFile(fileName, true);
            }
            catch (IOException e) {
                if (errs == null)
                    errs = new IgniteException("Failed to close index directory." +
                    " Some index readers weren't closed properly, that may leads memory leak.");

                errs.addSuppressed(e);
            }
        }

        assert fileMap.isEmpty();

        if (errs != null && !F.isEmpty(errs.getSuppressed()))
            throw errs;
    }

    /** {@inheritDoc} */
    @Override public Set<String> getPendingDeletions() throws IOException {
        return Collections.unmodifiableSet(pendingDeletions);
    }

    /** {@inheritDoc} */
    @Override public long ramBytesUsed() {
        ensureOpen();

        return sizeInBytes.get();
    }

    /** {@inheritDoc} */
    @Override public synchronized Collection<Accountable> getChildResources() {
        return Accountables.namedAccountables("file", new HashMap<>(fileMap));
    }

    /**
     * @return Memory.
     */
    GridUnsafeMemory memory() {
        return mem;
    }
}
