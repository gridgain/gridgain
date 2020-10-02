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
package org.apache.ignite.internal.processors.query.h2.disk;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import org.apache.ignite.internal.metric.SqlMemoryStatisticsHolder;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;

/**
 * File IO factory with tracking possibilities.
 */
public class TrackableFileIoFactory {
    /** Delegate factory. */
    private final FileIOFactory delegateFactory;

    /** Metrics holder. */
    private final SqlMemoryStatisticsHolder metrics;

    /**
     * @param factory Delegate factory.
     * @param metrics Metrics holder.
     */
    public TrackableFileIoFactory(FileIOFactory factory, SqlMemoryStatisticsHolder metrics) {
        this.delegateFactory = factory;
        this.metrics = metrics;
    }

    /**
     * Creates trackable file IO.
     * @param file File.
     * @param tracker Memory tracker..
     * @param modes Modes.
     * @return Trackable file IO.
     * @throws IOException If failed.
     */
    public FileIO create(File file, H2MemoryTracker tracker, OpenOption... modes) throws IOException {
        FileIO delegate = delegateFactory.create(file, modes);

        return new TrackableFileIO(delegate, metrics, tracker.createChildTracker());
    }

    /**
     * FileIO decorator for stats collecting.
     */
    private static class TrackableFileIO extends FileIODecorator {
        /** */
        private final SqlMemoryStatisticsHolder metrics;

        /** */
        private final H2MemoryTracker tracker;

        /**
         * @param delegate File I/O delegate
         */
        private TrackableFileIO(FileIO delegate, SqlMemoryStatisticsHolder metrics, H2MemoryTracker tracker) {
            super(delegate);

            this.metrics = metrics;
            this.tracker = tracker;
            this.tracker.incrementFilesCreated();
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf) throws IOException {
            int bytesRead = delegate.read(destBuf);

            trackReads(bytesRead);

            return bytesRead;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf, long position) throws IOException {
            int bytesRead = delegate.read(destBuf, position);

            trackReads(bytesRead);

            return bytesRead;
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] buf, int off, int len) throws IOException {
            int bytesRead = delegate.read(buf, off, len);

            trackReads(bytesRead);

            return bytesRead;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf) throws IOException {
            int bytesWritten = delegate.write(srcBuf);

            trackWrites(bytesWritten);

            return bytesWritten;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
            int bytesWritten = delegate.write(srcBuf, position);

            trackWrites(bytesWritten);

            return bytesWritten;
        }

        /** {@inheritDoc} */
        @Override public int write(byte[] buf, int off, int len) throws IOException {
            int bytesWritten = delegate.write(buf, off, len);

            trackWrites(bytesWritten);

            return bytesWritten;
        }

        /**
         * @param read Byte read.
         */
        private void trackReads(int read) {
            if (read > 0 && metrics != null)
                metrics.trackOffloadingRead(read);
        }

        /**
         * @param written Bytes written.
         */
        private void trackWrites(int written) {
            if (metrics != null)
                metrics.trackOffloadingWritten(written);

            if (tracker != null)
                tracker.spill(written);
        }

        /** {@inheritDoc} */
        @Override public void clear() throws IOException {
            super.clear();

            tracker.unspill(tracker.writtenOnDisk());
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            super.close();

            tracker.unspill(tracker.writtenOnDisk());

            tracker.close();
        }
    }
}
