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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link UserCommandExceptions}.
 */
@RunWith(MockitoJUnitRunner.class)
public class UserCommandExceptionsTest {
    /** Config. */
    private final IgniteConfiguration config = new IgniteConfiguration();

    /** Mock SPI. */
    @Mock
    private IgniteDiscoverySpi discoverySpi;

    /**
     * Tests that, if all nodes support the corresponding feature,
     * {@link UserCommandExceptions#invalidUserCommandException(String, DiscoverySpi)} creates an
     * {@link InvalidEnvironmentException}.
     */
    @Test
    public void createsInvalidUserCommandExceptionWhenFeatureIsSupported() {
        config.setDiscoverySpi(discoverySpi);
        when(discoverySpi.allNodesSupport(any(), any())).thenReturn(true);

        Exception ex = UserCommandExceptions.invalidUserCommandException("Hello", discoverySpi);

        assertThat(ex, isA(InvalidUserCommandException.class));
        assertThat(ex.getMessage(), is("Hello"));
    }

    /**
     * Tests that, if not all nodes support the corresponding feature,
     * {@link UserCommandExceptions#invalidUserCommandException(String, DiscoverySpi)} creates an
     * {@link IgniteException}.
     */
    @Test
    public void createsIgniteExceptionWhenInvalidUserCommandExceptionFeatureIsNotSupported() {
        config.setDiscoverySpi(discoverySpi);
        when(discoverySpi.allNodesSupport(any(), any())).thenReturn(false);

        Exception ex = UserCommandExceptions.invalidUserCommandException("Hello", discoverySpi);

        assertThat(ex, isA(IgniteException.class));
        assertThat(ex.getMessage(), is("Hello"));
    }

    /**
     * Tests that, if all nodes support the corresponding feature,
     * {@link UserCommandExceptions#invalidUserCommandException(String, DiscoverySpi)} creates an
     * {@link InvalidEnvironmentException}.
     */
    @Test
    public void createsInvalidUserCommandCheckedExceptionWhenFeatureIsSupported() {
        config.setDiscoverySpi(discoverySpi);
        when(discoverySpi.allNodesSupport(any(), any())).thenReturn(true);

        Exception ex = UserCommandExceptions.invalidUserCommandCheckedException("Hello", discoverySpi);

        assertThat(ex, isA(InvalidUserCommandCheckedException.class));
        assertThat(ex.getMessage(), is("Hello"));
    }

    /**
     * Tests that, if not all nodes support the corresponding feature,
     * {@link UserCommandExceptions#invalidUserCommandCheckedException(String, DiscoverySpi)} creates an
     * {@link IgniteCheckedException}.
     */
    @Test
    public void createsIgniteExceptionWhenInvalidUserCommandCheckedExceptionFeatureIsNotSupported() {
        config.setDiscoverySpi(discoverySpi);
        when(discoverySpi.allNodesSupport(any(), any())).thenReturn(false);

        Exception ex = UserCommandExceptions.invalidUserCommandCheckedException("Hello", discoverySpi);

        assertThat(ex, isA(IgniteCheckedException.class));
        assertThat(ex.getMessage(), is("Hello"));
    }

    /**
     * Tests that {@link UserCommandExceptions#causedByUserCommandException(Throwable)} returns {@code true} for
     * an instance of {@link InvalidUserCommandException}.
     */
    @Test
    public void causedByUserCommandExceptionRecognizesInvalidUserCommandException() {
        assertTrue(UserCommandExceptions.causedByUserCommandException(new InvalidUserCommandException("Oops")));
    }

    /**
     * Tests that {@link UserCommandExceptions#causedByUserCommandException(Throwable)} returns {@code true} for
     * an instance of {@link InvalidUserCommandCheckedException}.
     */
    @Test
    public void causedByUserCommandExceptionRecognizesInvalidUserCommandCheckedException() {
        assertTrue(UserCommandExceptions.causedByUserCommandException(new InvalidUserCommandCheckedException("Oops")));
    }

    /**
     * Tests that {@link UserCommandExceptions#causedByUserCommandException(Throwable)} returns {@code false} for
     * non-related exceptions.
     */
    @Test
    public void causedByUserCommandExceptionReturnsFalseForUnrelatedExceptions() {
        assertFalse(UserCommandExceptions.causedByUserCommandException(new Exception("Oops")));
    }
}
