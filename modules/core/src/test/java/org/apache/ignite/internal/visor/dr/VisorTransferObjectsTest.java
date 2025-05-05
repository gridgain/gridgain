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

package org.apache.ignite.internal.visor.dr;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.stream.IntStream;
import org.apache.ignite.internal.util.io.GridByteArrayInputStream;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.lang.String.valueOf;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.util.IgniteUtils.closeQuiet;

/**
 * Test for visor task objects serialization.
 */
public class VisorTransferObjectsTest extends GridCommonAbstractTest {
    /**
     * Test VisorDrCacheFSTTaskResult supports long UTF strings serialization.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testVisorDrCacheFSTTaskResultSerialization() throws Exception {
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;

        try {
            String bigUTF = IntStream.range(0, U.UTF_BYTE_LIMIT + 1)
                .map(i -> i % 0xffff).mapToObj(i -> valueOf((char)i)).collect(joining());

            VisorDrCacheFSTTaskResult original = new VisorDrCacheFSTTaskResult((byte)1, bigUTF);

            GridByteArrayOutputStream bos = new GridByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);

            oos.writeObject(original);

            GridByteArrayInputStream bis = new GridByteArrayInputStream(bos.internalArray());
            ois = new ObjectInputStream(bis);

            VisorDrCacheFSTTaskResult restored = (VisorDrCacheFSTTaskResult)ois.readObject();

            assertEquals(original.getProtocolVersion(), restored.getProtocolVersion());
            assertEquals(original.dataCenterId(), restored.dataCenterId());
            assertEquals(original.resultMessage(), restored.resultMessage());

            closeQuiet(oos);
            closeQuiet(ois);
        }
        finally {
            closeQuiet(oos);
            closeQuiet(ois);
        }
    }
}