/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestNode;
import org.junit.Test;

import static java.util.UUID.randomUUID;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for GridNioRecoveryDescriptor.
 */
public class GridNioRecoveryDescriptorTest {
    /** Descriptor under test. */
    private final GridNioRecoveryDescriptor descriptor = new GridNioRecoveryDescriptor(
        false,
        1024 * 1024,
        1000,
        new GridTestNode(randomUUID()),
        new NullLogger()
    );

    /** */
    @Test
    public void freshDescriptorHasZeroUnackedMessages() {
        assertThat(descriptor.messageRequestsCount(), is(0));
    }

    /** */
    @Test
    public void additionIncreasesUnackedMessagesCount() {
        SessionWriteRequest request = mock(SessionWriteRequest.class);

        when(request.skipRecovery()).thenReturn(false);

        descriptor.add(request);
        assertThat(descriptor.messageRequestsCount(), is(1));

        descriptor.add(request);
        assertThat(descriptor.messageRequestsCount(), is(2));
    }

    /** */
    @Test
    public void acknowledgementDecreasesUnackedMessagesCount() {
        SessionWriteRequest request = mock(SessionWriteRequest.class);

        when(request.skipRecovery()).thenReturn(false);

        descriptor.add(request);
        descriptor.add(request);
        assertThat(descriptor.messageRequestsCount(), is(2));

        descriptor.ackReceived(1);
        assertThat(descriptor.messageRequestsCount(), is(1));

        descriptor.ackReceived(2);
        assertThat(descriptor.messageRequestsCount(), is(0));
    }

    /** */
    @Test
    public void nodeLeaveZeroesOutUnackedMessagesCount() {
        SessionWriteRequest request = mock(SessionWriteRequest.class);

        when(request.skipRecovery()).thenReturn(false);

        descriptor.add(request);
        descriptor.add(request);
        assertThat(descriptor.messageRequestsCount(), is(2));

        descriptor.onNodeLeft();

        assertThat(descriptor.messageRequestsCount(), is(0));
    }

    /** */
    @Test
    public void releaseZeroesOutUnackedMessagesCount() {
        SessionWriteRequest request = mock(SessionWriteRequest.class);

        when(request.skipRecovery()).thenReturn(false);

        // We have to trigger 'node left' first, otherwise 'release' will not empty the unacked queue.
        descriptor.onNodeLeft();

        descriptor.add(request);
        descriptor.add(request);
        assertThat(descriptor.messageRequestsCount(), is(2));

        descriptor.release();

        assertThat(descriptor.messageRequestsCount(), is(0));
    }
}
