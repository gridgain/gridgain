/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.inference.storage.model;

import java.util.concurrent.locks.Lock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests for {@link DefaultModelStorage}.
 */
public class DefaultModelStorageTest extends AbstractModelStorageTest {
    /** {@inheritDoc} */
    @Override ModelStorage getModelStorage() {
        ModelStorageProvider provider = new LocalModelStorageProvider();
        return new DefaultModelStorage(provider);
    }

    /** */
    @Test
    public void testSynchronize() {
        Lock[] locks = new Lock[10];
        for (int i = 0; i < locks.length; i++)
            locks[i] = mock(Lock.class);

        DefaultModelStorage.synchronize(() -> {}, locks);

        for (Lock lock : locks) {
            verify(lock, times(1)).lock();
            verify(lock, times(1)).unlock();
            verifyNoMoreInteractions(lock);
        }
    }

    /** */
    @Test
    public void testSynchronizeWithExceptionInTask() {
        Lock[] locks = new Lock[10];
        for (int i = 0; i < locks.length; i++)
            locks[i] = mock(Lock.class);

        RuntimeException ex = new RuntimeException();

        try {
            DefaultModelStorage.synchronize(() -> { throw ex; }, locks);
            fail();
        }
        catch (RuntimeException e) {
            assertEquals(ex, e);
        }

        for (Lock lock : locks) {
            verify(lock, times(1)).lock();
            verify(lock, times(1)).unlock();
            verifyNoMoreInteractions(lock);
        }
    }

    /** */
    @Test
    public void testSynchronizeWithExceptionInLock() {
        Lock[] locks = new Lock[10];
        for (int i = 0; i < locks.length; i++)
            locks[i] = mock(Lock.class);

        RuntimeException ex = new RuntimeException();

        doThrow(ex).when(locks[5]).lock();

        try {
            DefaultModelStorage.synchronize(() -> {}, locks);
            fail();
        }
        catch (RuntimeException e) {
            assertEquals(ex, e);
        }

        for (int i = 0; i < locks.length; i++) {
            if (i <= 4) {
                verify(locks[i], times(1)).lock();
                verify(locks[i], times(1)).unlock();
            }
            else if (i > 5) {
                verify(locks[i], times(0)).lock();
                verify(locks[i], times(0)).unlock();
            }
            else {
                verify(locks[i], times(1)).lock();
                verify(locks[i], times(0)).unlock();
            }

            verifyNoMoreInteractions(locks[i]);
        }
    }

    /** */
    @Test
    public void testSynchronizeWithExceptionInUnlock() {
        Lock[] locks = new Lock[10];
        for (int i = 0; i < locks.length; i++)
            locks[i] = mock(Lock.class);

        RuntimeException ex = new RuntimeException();

        doThrow(ex).when(locks[5]).unlock();

        try {
            DefaultModelStorage.synchronize(() -> {}, locks);
            fail();
        }
        catch (RuntimeException e) {
            assertEquals(ex, e);
        }

        for (Lock lock : locks) {
            verify(lock, times(1)).lock();
            verify(lock, times(1)).unlock();
            verifyNoMoreInteractions(lock);
        }
    }
}
