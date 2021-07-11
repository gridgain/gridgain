/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.platform;

import org.apache.ignite.cache.CacheEntryProcessor;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/**
 * Entry processor that adds argument to cache entry and returns PlatformComputeBinarizable.
 */
public class PlatformAddArgEntryProcessorBinarizable
        implements CacheEntryProcessor<Object, Long, PlatformComputeBinarizable> {
    @Override public PlatformComputeBinarizable process(MutableEntry<Object, Long> mutableEntry, Object... args)
            throws EntryProcessorException {
        Long val = (Long)args[0];
        Long res = mutableEntry.getValue();
        res = res == null ? val : res + val;

        mutableEntry.setValue(res);

        return new PlatformComputeBinarizable(res.intValue());
    }
}
