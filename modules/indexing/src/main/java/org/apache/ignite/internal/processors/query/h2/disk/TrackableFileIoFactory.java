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
import org.apache.ignite.internal.processors.query.h2.IgniteTrace;

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
     * @param trace Trace object.
     * @param modes Modes.
     * @return Trackable file IO.
     * @throws IOException If failed.
     */
    public FileIO create(File file, IgniteTrace trace, OpenOption... modes) throws IOException {
        FileIO delegate = delegateFactory.create(file, modes);

        return new TrackableFileIO(delegate, metrics, trace);
    }

    /**
     * FileIO decorator for stats collecting.
     */
    private static class TrackableFileIO extends FileIODecorator {
        /** */
        private final SqlMemoryStatisticsHolder metrics;

        /** */
        private final IgniteTrace trace;

        /** */
        private long totalBytesRead;

        /** */
        private long  totalBytesWritten;

        /**
         * @param delegate File I/O delegate
         */
        private TrackableFileIO(FileIO delegate, SqlMemoryStatisticsHolder metrics, IgniteTrace trace) {
            super(delegate);

            this.metrics = metrics;
            this.trace = trace;
            this.trace.addFilesCreated(1);
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf) throws IOException {
            int bytesRead = delegate.read(destBuf);

            if (bytesRead > 0)
                totalBytesRead += bytesRead;

            return bytesRead;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf, long position) throws IOException {
            int bytesRead = delegate.read(destBuf, position);

            if (bytesRead > 0)
                totalBytesRead += bytesRead;

            return bytesRead;
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] buf, int off, int len) throws IOException {
            int bytesRead = delegate.read(buf, off, len);

            if (bytesRead > 0)
                totalBytesRead += bytesRead;

            return bytesRead;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf) throws IOException {
            int bytesWritten = delegate.write(srcBuf);

            if (bytesWritten > 0)
                totalBytesWritten += bytesWritten;

            return bytesWritten;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
            int bytesWritten = delegate.write(srcBuf, position);

            if (bytesWritten > 0)
                totalBytesWritten += bytesWritten;

            return bytesWritten;
        }

        /** {@inheritDoc} */
        @Override public int write(byte[] buf, int off, int len) throws IOException {
            int bytesWritten = delegate.write(buf, off, len);

            if (bytesWritten > 0)
                totalBytesWritten += bytesWritten;

            return bytesWritten;
        }

        @Override public void close() throws IOException {
            if (metrics != null) {
                metrics.trackOffloadingWritten(totalBytesWritten);
                metrics.trackOffloadingRead(totalBytesRead);
            }

            if (trace != null)
                trace.addOffloadedBytes(totalBytesWritten);

            super.close();
        }
    }
}
