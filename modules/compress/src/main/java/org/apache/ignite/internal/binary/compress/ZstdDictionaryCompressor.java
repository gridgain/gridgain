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

package org.apache.ignite.internal.binary.compress;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.resources.LoggerResource;

/** */
public class ZstdDictionaryCompressor extends CompressorAdapter {
    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** */
    private final AtomicInteger samplesToCollect = new AtomicInteger();

    /** */
    private final AtomicLong bufToCollect = new AtomicLong();

    /** */
    private final Queue<byte[]> samples = new LinkedBlockingQueue<>();

    /** */
    private volatile ZstdDictCompress compressor;

    /** */
    private volatile ZstdDictDecompress decompressor;

    /** */
    private final Lock dictLock = new ReentrantLock();

    /** */
    private volatile int level;

    /** */
    private volatile int dictSize;

    /** */
    public ZstdDictionaryCompressor() {
        level = 2;
        dictSize = 1024;

        samplesToCollect.set(16384);
        bufToCollect.set(4L * 1024 * 1024);

        samples.clear();
    }

    /** */
    public byte[] tryCompress(byte[] input) {
        ZstdDictCompress compressor0 = this.compressor;

        if (compressor0 != null) {
            byte[] compressed = compress(input, compressor0);

            return appraiseAndAddHeader(input, compressed, 1);
        }

        int iv = samplesToCollect.decrementAndGet();

        if (iv > 0)
            dictionarize(input);

        else if (iv == 0) {
            dictLock.lock();
            int totalLen = 0;
            for (byte[] sample : samples)
                totalLen += sample.length;

            ZstdDictTrainer trainer = new ZstdDictTrainer(totalLen, dictSize);

            for (byte[] sample : samples)
                trainer.addSample(sample);

            byte[] dictionary = trainer.trainSamples();

            this.decompressor = new ZstdDictDecompress(dictionary);
            this.compressor = new ZstdDictCompress(dictionary, level);

            dictLock.unlock();
        }

        return null;
    }

    /** */
    public static byte[] compress(byte[] src, ZstdDictCompress dict) {
        long maxDstSize = Zstd.compressBound(src.length);

        if (maxDstSize > Integer.MAX_VALUE)
            throw new IgniteException("Max output size is greater than MAX_INT");

        byte[] dst = new byte[(int) maxDstSize + 1];

        long size = Zstd.compressFastDict(dst,1, src,0,src.length, dict);

        if (Zstd.isError(size))
            throw new IgniteException(Zstd.getErrorName(size));

        return Arrays.copyOfRange(dst, 0, (int) size + 1);
    }

    private void dictionarize(byte[] uncompressed) {
        if (uncompressed.length == 0 || !dictLock.tryLock()
            || bufToCollect.addAndGet(-uncompressed.length) <= 0 )
            return;

        samples.add(Arrays.copyOf(uncompressed, uncompressed.length));

        dictLock.unlock();
    }

    /** */
    @Override protected IgniteLogger log() {
        return log;
    }

    @Override protected ZstdDictDecompress dictionary(byte dict) {
        assert decompressor != null : "Decompressor is not ready";

        return decompressor;
    }
}
