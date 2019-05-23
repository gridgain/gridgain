package org.apache.ignite.internal.binary.nextgen;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.internal.binary.BinaryPrimitives;

public class BikeBuilder {
    private final int ncols;
    private final BitSet nullsBitSet;
    private final int nullsBytes;
    private final byte[] varlens;
    private final ByteArrayOutputStream data = new ByteArrayOutputStream();
    private int cur;
    private int curVarlenOff;
    private int curOff;
    private final int binaryTypeId;

    public BikeBuilder(int ncols, int binaryTypeId) {
        this.ncols = ncols;

        nullsBytes = ((ncols - 1) >>> 3) + 1;
        this.binaryTypeId = binaryTypeId;
        nullsBitSet = new BitSet(nullsBytes);

        varlens = new byte[ncols * 2];

        curOff = 0;
    }

    private static final Map<Class<?>, Function<Object, byte[]>> serializers;

    static {
        serializers = new HashMap<>();
        serializers.put(String.class, x -> ((String)x).getBytes(StandardCharsets.UTF_8));
        serializers.put(Short.class, x -> {
            byte[] b = new byte[2];
            BinaryPrimitives.writeShort(b, 0, (Short)x);
            return b;
        });
        serializers.put(Integer.class, x -> {
            byte[] b = new byte[4];
            BinaryPrimitives.writeInt(b, 0, (Integer)x);
            return b;
        });
        serializers.put(Long.class, x -> {
            byte[] b = new byte[8];
            BinaryPrimitives.writeLong(b, 0, (Long)x);
            return b;
        });
        serializers.put(BigDecimal.class, x -> {
            BigDecimal bd = (BigDecimal)x;
            byte[] intBytes = ((BigDecimal)x).unscaledValue().toByteArray();

            byte[] b = new byte[4 + intBytes.length];
            BinaryPrimitives.writeInt(b, 0, bd.scale());
            System.arraycopy(intBytes, 0, b, 4, intBytes.length);

            return b;
        });
        serializers.put(java.sql.Date.class, x -> {
            byte[] b = new byte[8];
            BinaryPrimitives.writeLong(b, 0, ((java.sql.Date)x).getTime());
            return b;
        });
        serializers.put(byte[].class, x -> (byte[])x);
    }

    public BikeBuilder append(Object v) {
        byte[] bytes;

        if (v != null) {
            nullsBitSet.set(cur);
            bytes = serialize(v);

            data.write(bytes, 0, bytes.length);

//            BinaryPrimitives.writeShort(varlens, curVarlenOff, (short)bytes.length);
//            curVarlenOff+=2;

            BinaryPrimitives.writeShort(varlens, curVarlenOff, (short)curOff);
            curVarlenOff+=2;
            // t0d0 write absolute offsets?
            curOff+=bytes.length;
        }

        cur++;
        return this;
    }

    private static byte[] serialize(Object v) {
        Function<Object, byte[]> ser = serializers.get(v.getClass());
        if (ser == null)
            throw new RuntimeException("Failed to find serializer for " + v + ": " + v.getClass());
        return ser.apply(v);
    }

    public byte[] build() {
        if (cur != ncols)
            throw new RuntimeException();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] nulls0 = nullsBitSet.toByteArray();
        byte[] nulls = new byte[nullsBytes];
        System.arraycopy(nulls0, 0, nulls, 0, nulls0.length);

        byte[] typeIdBytes = new byte[4];
        BinaryPrimitives.writeInt(typeIdBytes, 0, binaryTypeId);
        baos.write(typeIdBytes, 0, typeIdBytes.length);

        baos.write(nulls.length + 1);
        baos.write(nulls, 0, nulls.length);

        byte[] lenlen = new byte[2];
        BinaryPrimitives.writeShort(lenlen, 0, (short)(curVarlenOff + 2));
        baos.write(lenlen, 0, lenlen.length);
        baos.write(varlens, 0, curVarlenOff);

        try {
            data.writeTo(baos);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return baos.toByteArray();
    }
}
