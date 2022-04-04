/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.tests.p2p.classloadproblem;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * {@link CacheEntryProcessor} implementation that breaks p2p class-loading and causes another class to be loaded
 * using p2p when its process() method is called.
 * <p>
 * Used to test the situation when p2p class-loading fails due to a peer failing or leaving.
 * <p>
 * NB: The process() method should only be called on an instance of this class loaded by a P2P class-loader.
 */
public class CacheEntryProcessorCausingP2PClassLoadProblem implements CacheEntryProcessor<Integer, String, String> {
    /***/
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public String process(MutableEntry<Integer, String> entry, Object... arguments)
        throws EntryProcessorException {
        P2PClassLoadingProblems.triggerP2PClassLoadingProblem(getClass(), ignite);
        return null;
    }
}
