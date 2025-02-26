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

package org.apache.ignite.internal.processors.security;

import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link IgniteSecurityProcessor}.
 */
public class IgniteSecurityProcessorTest {
    /**
     * Checks that {@link IgniteSecurityProcessor#withContext(UUID)} swithes to "deny all policy" in case node ID is unknown.
     */
    @Test
    public void testSwitchToDenyAllPolicyIfNodeNotFoundInDiscoCache() {
        GridKernalContext ctx = mock(GridKernalContext.class);
        when(ctx.config()).thenReturn(new IgniteConfiguration());
        when(ctx.discovery()).thenReturn(mock(GridDiscoveryManager.class));
        when(ctx.log(any(Class.class))).thenReturn(mock(IgniteLogger.class));

        MarshallerContextImpl mockMarshallerContext = mock(MarshallerContextImpl.class);
        when(mockMarshallerContext.jdkMarshaller()).thenReturn(new JdkMarshaller());

        when(ctx.marshallerContext()).thenReturn(mockMarshallerContext);

        GridSecurityProcessor secPrc = mock(GridSecurityProcessor.class);

        IgniteSecurityProcessor realIgniteSecProc = new IgniteSecurityProcessor(ctx, secPrc);

        IgniteSecurityProcessor igniteSecProc = spy(realIgniteSecProc);
        OperationSecurityContext secCtxMock = mock(OperationSecurityContext.class);
        ArgumentCaptor<SecurityContext> arg = ArgumentCaptor.forClass(SecurityContext.class);
        doReturn(secCtxMock).when(igniteSecProc).withContext(any(SecurityContext.class));

        UUID subjectId = UUID.randomUUID();

        igniteSecProc.withContext(subjectId);

        verify(igniteSecProc).withContext(arg.capture());

        assertFalse(arg.getValue().subject().permissions().defaultAllowAll());
        assertFalse(arg.getValue().systemOperationAllowed(null));
        assertFalse(arg.getValue().cacheOperationAllowed(null, null));
        assertFalse(arg.getValue().serviceOperationAllowed(null, null));
        assertFalse(arg.getValue().taskOperationAllowed(null, null));
    }
}
