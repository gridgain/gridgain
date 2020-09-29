package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class HasherSelfTest extends GridCommonAbstractTest {

    /**
     * Create hash by arrays with different size and check that hash differ.
     */
    @Test
    public void testCornerCaseSizedArray() {
        Hasher h = new Hasher();
        List<Long> list = new ArrayList<>();

        list.add(h.fastHash(new byte[]{}));
        list.add(h.fastHash(new byte[]{1}));
        list.add(h.fastHash(new byte[]{1,2,3,4}));
        list.add(h.fastHash(new byte[]{1,2,3,4,5,6,7}));
        list.add(h.fastHash(new byte[]{1,2,3,4,5,6,7,8}));
        list.add(h.fastHash(new byte[]{1,2,3,4,5,6,7,8,9}));
        list.add(h.fastHash(new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}));
        list.add(h.fastHash(new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}));
        list.add(h.fastHash(new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17}));

        Set<Long> set = new HashSet<>();
        set.addAll(list);

        assertEquals(list.size(), set.size());
    }

    /**
     * Test that hashes of few random generated arrays are the same if generated multiple times.
     */
    @Test
    public void testRandomHash() {
        Hasher h = new Hasher();
        Random r = new Random();

        int testSize = 100;
        Map<byte[], Long> hashes = new HashMap<>(testSize);

        for (int i = 0; i < testSize; i++) {
            byte arr[] = new byte[r.nextInt(1000)];
            r.nextBytes(arr);
            hashes.put(arr, h.fastHash(arr));
        }

        for(Map.Entry<byte[], Long> entry : hashes.entrySet())
            assertEquals(entry.getValue().longValue(), h.fastHash(entry.getKey()));

    }


}
