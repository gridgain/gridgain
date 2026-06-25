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

package org.apache.ignite.platform;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryClassDescriptor;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.TestCachingMetadataHandler;
import org.apache.ignite.internal.processors.platform.websession.PlatformDotNetSessionLockResult;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for registering PlatformDotNetSessionLockResult class.
 */
public class BinaryContextPlatformDotNetSessionLockResultPredefinedTest {

    /** Counts how many times {@link TrackedResult#TrackedResult()} is called. */
    private static final AtomicInteger ctorInvocations = new AtomicInteger();

    /**
     * Subclass whose no-arg constructor increments {@link #ctorInvocations} so the test
     * can verify registration does not eagerly instantiate the type.
     */
    public static class TrackedResult extends PlatformDotNetSessionLockResult {
        /** */
        public TrackedResult() {
            ctorInvocations.incrementAndGet();
        }
    }

    /** */
    @Test
    public void testBinaryContextRegisterPredefinedTypes() throws Exception {
        ctorInvocations.set(0);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        igniteCfg.setIgniteInstanceName("test");

        BinaryContext binCtx = new BinaryContext(new TestCachingMetadataHandler(), igniteCfg, null);

        BinaryClassDescriptor descriptorOne = binCtx.registerPredefinedType(TrackedResult.class, 0);

        binCtx = new BinaryContext(new TestCachingMetadataHandler(), igniteCfg, null);

        BinaryClassDescriptor descriptorTwo = binCtx.registerPredefinedType(TrackedResult.class, 0);

        Field f = BinaryClassDescriptor.class.getDeclaredField("ctor");
        f.setAccessible(true);

        Constructor ctorOne = (Constructor) f.get(descriptorOne);

        Constructor ctorTwo = (Constructor) f.get(descriptorTwo);

        Assert.assertEquals(ctorOne, ctorTwo);
        // Registration must store the constructor, not call it.
        Assert.assertEquals(0, ctorInvocations.get());
    }
}
