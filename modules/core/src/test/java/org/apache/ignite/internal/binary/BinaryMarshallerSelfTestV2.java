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

package org.apache.ignite.internal.binary;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.CUR_PROTO_VER;

/**
 * Binary marshaller tests for protocol version 2.
 */
public class BinaryMarshallerSelfTestV2 extends BinaryMarshallerSelfTest {
    /**
     * @return Binary protocol version.
     */
    @Override protected byte protocolVersion() {
        return 2;
    }

    /**
     * Verify BO API extension for update time with provided object.
     *
     * @param marsh Marshaller.
     * @param obj Test object.
     */
    private void verify(BinaryMarshaller marsh, Object obj) throws IgniteCheckedException {
        BinaryObjectExImpl binObj = marshal(obj, marsh);

        BinaryObjectExImpl binWithTs = buildLatest(binObj.toBuilder().setUpdateTime(100L));

        Assert.assertEquals(-1L, binObj.updateTime());
        Assert.assertEquals(100L, binWithTs.updateTime());
        Assert.assertEquals(binObj, binWithTs);
        Assert.assertEquals(obj, marsh.unmarshal(binWithTs.array(), null));

        BinaryObjectExImpl binWithTsCp = buildLatest(binWithTs.toBuilder());

        Assert.assertEquals(100L, binWithTsCp.updateTime());
        Assert.assertEquals(binObj, binWithTsCp);
        Assert.assertEquals(obj, marsh.unmarshal(binWithTsCp.array(), null));

        BinaryObjectExImpl binWithTsUpd = buildLatest(binWithTs.toBuilder().setUpdateTime(200L));

        Assert.assertEquals(200L, binWithTsUpd.updateTime());
        Assert.assertEquals(binObj, binWithTsUpd);
        Assert.assertEquals(obj, marsh.unmarshal(binWithTsUpd.array(), null));

        BinaryObjectExImpl binWithoutTs = buildLatest(binWithTsUpd.toBuilder().removeUpdateTime());

        Assert.assertEquals(-1L, binWithoutTs.updateTime());
        Assert.assertEquals(binObj, binWithoutTs);
        Assert.assertEquals(obj, marsh.unmarshal(binWithoutTs.array(), null));
    }

    /**
     * Ensure that update time can be seet to objects of different types.
     */
    @Test
    public void testUpdateTime() throws IgniteCheckedException {
        BinaryMarshaller marsh = binaryMarshaller(
            Arrays.asList(
                new BinaryTypeConfiguration(Value.class.getName()),
                new BinaryTypeConfiguration(ObjectRaw.class.getName()),
                new BinaryTypeConfiguration(ObjectWithRaw.class.getName()),
                new BinaryTypeConfiguration(SimpleObject.class.getName())
            ),
            Collections.singletonList(Wrapper.class.getName())
        );

        Object[] objs = new Object[] {
            new Value(1),
            new ObjectRaw(1, 2),
            new ObjectWithRaw(1, 2),
            simpleObject(),
            new Wrapper("42")
        };

        for (Object obj : objs)
            verify(marsh, obj);
    }

    /**
     * Build object of {@link GridBinaryMarshaller#CUR_PROTO_VER} version from provided builder.
     *
     * @param bldr Bilder to build.
     * @return Binary object with protocol version {@link GridBinaryMarshaller#CUR_PROTO_VER}.
     */
    private BinaryObjectExImpl buildLatest(BinaryObjectBuilder bldr) {
        return (BinaryObjectExImpl)((BinaryObjectBuilderImpl)bldr).build(CUR_PROTO_VER);
    }
}
