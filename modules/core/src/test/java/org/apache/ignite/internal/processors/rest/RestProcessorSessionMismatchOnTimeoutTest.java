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

package org.apache.ignite.internal.processors.rest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test reproduces GG-26664 when snapshot-utility stopped sending topology and progress update requests during some
 * period of time which forces server to remove session because of timeout
 */
public class RestProcessorSessionMismatchOnTimeoutTest {

    /** Unsupported case error message. */
    private static final String UNSUPPORTED_CASE_ERROR_MESSAGE_BEGIN =
        "Failed to handle request - unsupported case (mismatched clientId and session token)";

    /** Illagal exception message*/
    private static final String AUTH_EXCEPTION_EXPECTED = "IgniteAuthenticationException was expected";

    /** Rest processor to test */
    private GridRestProcessor processor;

    /** Private method "session" of {@link GridRestProcessor} */
    private Method sesMtdInternal;

    /**
     * Preparation for test
     */
    @Before
    public void before() throws NoSuchMethodException {
        final IgniteLogger log = mock(IgniteLogger.class);
        when(log.getLogger(anyString())).thenReturn(log);

        processor = new GridRestProcessor(new GridTestKernalContext(log));
        sesMtdInternal = processor.getClass().getDeclaredMethod("session", GridRestRequest.class);
        sesMtdInternal.setAccessible(true);
    }

    /**
     * Check that correct error is thrown when rest session was timed out to forces client to re-authenticate himself
     */
    @Test
    public void throwAuthExceptionWhenSessionTimedOut() {
        final GridRestRequest req = new GridRestRequest();
        req.clientId(UUID.randomUUID());
        req.sessionToken(U.uuidToBytes(UUID.randomUUID()));

        try {
            sesMtdInternal.invoke(processor, req);
        }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof IgniteAuthenticationException) {
                assertTrue(e.getCause().getMessage().startsWith(UNSUPPORTED_CASE_ERROR_MESSAGE_BEGIN));
                return;
            }

            fail(AUTH_EXCEPTION_EXPECTED);
        }
        catch (Exception e) {
            fail(AUTH_EXCEPTION_EXPECTED);
        }

        fail(AUTH_EXCEPTION_EXPECTED);
    }
}
