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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/**
 * Tests of {@link ClientExceptions}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ClientExceptionsTest {
    /***/
    @Mock
    private ClusterNode node;

    /***/
    @Test
    public void detectsClientNodeTopologyException() {
        when(node.isClient()).thenReturn(true);

        assertTrue(ClientExceptions.isClientNodeTopologyException(clusterTopologyCheckedException(), node));
    }

    /***/
    @Test
    public void doesNotDetectsClientNodeTopologyExceptionForNonClient() {
        when(node.isClient()).thenReturn(false);

        assertFalse(ClientExceptions.isClientNodeTopologyException(clusterTopologyCheckedException(), node));
    }

    /***/
    @Test
    public void doesNotDetectClientNodeTopologyExceptionForOtherExceptions() {
        lenient().when(node.isClient()).thenReturn(true);

        assertFalse(ClientExceptions.isClientNodeTopologyException(new IgniteCheckedException(), node));
    }

    /***/
    @NotNull
    private Exception clusterTopologyCheckedException() {
        return new ClusterTopologyCheckedException(
            "Failed to wait for establishing inverse connection (node left topology): 67cf0e5e-974c-463a-a1f2-915fe3cdd3e7"
        );
    }
}
