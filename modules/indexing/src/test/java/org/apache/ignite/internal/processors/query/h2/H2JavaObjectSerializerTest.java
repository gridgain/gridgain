package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

/**
 * Unit tests for H2JavaObjectSerializer.
 */
public class H2JavaObjectSerializerTest extends GridCommonAbstractTest {
    private H2JavaObjectSerializer serializer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        serializer = new H2JavaObjectSerializer();
    }

    @Test
    public void testSerializeBinaryObject() throws IOException {
        BinaryObject obj = mock(BinaryObject.class);
        when(obj.typeId()).thenReturn(123);

        IOException exception = assertThrows(IOException.class, () -> {
            serializer.serialize(obj);
        });

        assertEquals("Failed to serialize BinaryObject with typeId: 123", exception.getMessage());
    }

    @Test
    public void testSerializeNonBinaryObject() throws IOException {
        Object obj = new Object();

        IOException exception = assertThrows(IOException.class, () -> {
            serializer.serialize(obj);
        });

        assertEquals("Failed to Serialize object: " + obj.getClass().getName(), exception.getMessage());
    }

    @Test
    public void testDeserialize() throws IOException {
        byte[] bytes = new byte[]{1, 2, 3};

        IOException exception = assertThrows(IOException.class, () -> {
            serializer.deserialize(bytes);
        });

        assertEquals("Failed to deserialize data: " + Arrays.toString(bytes), exception.getMessage());
    }
}
