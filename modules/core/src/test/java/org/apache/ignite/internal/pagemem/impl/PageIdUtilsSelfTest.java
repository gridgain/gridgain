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

package org.apache.ignite.internal.pagemem.impl;

import java.util.Random;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class PageIdUtilsSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRotatePageId() throws Exception {
        assertEquals(0x0102FFFFFFFFFFFFL, PageIdUtils.rotatePageId(0x0002FFFFFFFFFFFFL));
        assertEquals(0x0B02FFFFFFFFFFFFL, PageIdUtils.rotatePageId(0x0A02FFFFFFFFFFFFL));
        assertEquals(0x1002FFFFFFFFFFFFL, PageIdUtils.rotatePageId(0x0F02FFFFFFFFFFFFL));
        assertEquals(0xFF02FFFFFFFFFFFFL, PageIdUtils.rotatePageId(0xFE02FFFFFFFFFFFFL));
        assertEquals(0x0012FFFFFFFFFFFFL, PageIdUtils.rotatePageId(0xFF02FFFFFFFFFFFFL));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEffectivePageId() throws Exception {
        assertEquals(0x0000FFFFFFFFFFFFL, PageIdUtils.effectivePageId(0x0002FFFFFFFFFFFFL));
        assertEquals(0x0000FFFFFFFFFFFFL, PageIdUtils.effectivePageId(0x0A02FFFFFFFFFFFFL));
        assertEquals(0x0000FFFFFFFFFFFFL, PageIdUtils.effectivePageId(0x0F02FFFFFFFFFFFFL));
        assertEquals(0x0000FFFFFFFFFFFFL, PageIdUtils.effectivePageId(0xFF02FFFFFFFFFFFFL));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLinkConstruction() throws Exception {
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.link(0x0FFFFFFFFFFFFFL, 0));
        assertEquals(0x010FFFFFFFFFFFFFL, PageIdUtils.link(0x0FFFFFFFFFFFFFL, 1));

        assertEquals(0x0000000000000000L, PageIdUtils.link(0, 0));
        assertEquals(0x0100000000000000L, PageIdUtils.link(0, 1));

        assertEquals(0xF000000000000000L, PageIdUtils.link(0, 0xF0));
        assertEquals(0xF00FFFFFFFFFFFFFL, PageIdUtils.link(0x0FFFFFFFFFFFFFL, 0xF0));

        assertEquals(0xFE00000000000000L, PageIdUtils.link(0, 0xFE));
        assertEquals(0xFE0FFFFFFFFFFFFFL, PageIdUtils.link(0x0FFFFFFFFFFFFFL, 0xFE));

        assertEquals(0x0F00000000000000L, PageIdUtils.link(0, 0xF));
        assertEquals(0x0F0FFFFFFFFFFFFFL, PageIdUtils.link(0x0FFFFFFFFFFFFFL, 0xF));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOffsetExtraction() throws Exception {
        assertEquals(0, PageIdUtils.itemId(0x000FFFFFFFFFFFFFL));
        assertEquals(1, PageIdUtils.itemId(0x010FFFFFFFFFFFFFL));

        assertEquals(0, PageIdUtils.itemId(0x0000000000000000L));
        assertEquals(1, PageIdUtils.itemId(0x0100000000000000L));

        assertEquals(0xFA, PageIdUtils.itemId(0xFA00000000000000L));
        assertEquals(0xFA, PageIdUtils.itemId(0xFA0FFFFFFFFFFFFFL));

        assertEquals(0xF, PageIdUtils.itemId(0x0F00000000000000L));
        assertEquals(0xF, PageIdUtils.itemId(0x0F0FFFFFFFFFFFFFL));

        assertEquals(0xF0, PageIdUtils.itemId(0xF000000000000000L));
        assertEquals(0xF0, PageIdUtils.itemId(0xF00FFFFFFFFFFFFFL));

        assertEquals(0x1FF, PageIdUtils.itemId(0xFF1FFFFFFFFFFFFFL));
        assertEquals(0xFFF, PageIdUtils.itemId(0xFFFFFFFFFFFFFFFFL));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageIdFromLink() throws Exception {
        assertEquals(0x00FFFFFFFFFFFFFFL, PageIdUtils.pageId(0x00FFFFFFFFFFFFFFL));

        assertEquals(0x0001FFFFFFFFFFFFL, PageIdUtils.pageId(0x0001FFFFFFFFFFFFL));
        assertEquals(0x0001FFFFFFFFFFFFL, PageIdUtils.pageId(0x1001FFFFFFFFFFFFL));
        assertEquals(0x0001FFFFFFFFFFFFL, PageIdUtils.pageId(0x0101FFFFFFFFFFFFL));
        assertEquals(0x0001FFFFFFFFFFFFL, PageIdUtils.pageId(0x1101FFFFFFFFFFFFL));
        assertEquals(0x0001FFFFFFFFFFFFL, PageIdUtils.pageId(0x8001FFFFFFFFFFFFL));
        assertEquals(0x0001FFFFFFFFFFFFL, PageIdUtils.pageId(0x8801FFFFFFFFFFFFL));
        assertEquals(0x0001FFFFFFFFFFFFL, PageIdUtils.pageId(0x0801FFFFFFFFFFFFL));
        assertEquals(0x0001FFFFFFFFFFFFL, PageIdUtils.pageId(0xFF01FFFFFFFFFFFFL));

        assertEquals(0x0002FFFFFFFFFFFFL, PageIdUtils.pageId(0x0002FFFFFFFFFFFFL));
        assertEquals(0x1002FFFFFFFFFFFFL, PageIdUtils.pageId(0x1002FFFFFFFFFFFFL));
        assertEquals(0x0102FFFFFFFFFFFFL, PageIdUtils.pageId(0x0102FFFFFFFFFFFFL));
        assertEquals(0x1102FFFFFFFFFFFFL, PageIdUtils.pageId(0x1102FFFFFFFFFFFFL));
        assertEquals(0x8002FFFFFFFFFFFFL, PageIdUtils.pageId(0x8002FFFFFFFFFFFFL));
        assertEquals(0x8802FFFFFFFFFFFFL, PageIdUtils.pageId(0x8802FFFFFFFFFFFFL));
        assertEquals(0x0802FFFFFFFFFFFFL, PageIdUtils.pageId(0x0802FFFFFFFFFFFFL));
        assertEquals(0xFF02FFFFFFFFFFFFL, PageIdUtils.pageId(0xFF02FFFFFFFFFFFFL));

        assertEquals(0x0004FFFFFFFFFFFFL, PageIdUtils.pageId(0x0004FFFFFFFFFFFFL));
        assertEquals(0x1004FFFFFFFFFFFFL, PageIdUtils.pageId(0x1004FFFFFFFFFFFFL));
        assertEquals(0x0104FFFFFFFFFFFFL, PageIdUtils.pageId(0x0104FFFFFFFFFFFFL));
        assertEquals(0x1104FFFFFFFFFFFFL, PageIdUtils.pageId(0x1104FFFFFFFFFFFFL));
        assertEquals(0x8004FFFFFFFFFFFFL, PageIdUtils.pageId(0x8004FFFFFFFFFFFFL));
        assertEquals(0x8804FFFFFFFFFFFFL, PageIdUtils.pageId(0x8804FFFFFFFFFFFFL));
        assertEquals(0x0804FFFFFFFFFFFFL, PageIdUtils.pageId(0x0804FFFFFFFFFFFFL));
        assertEquals(0xFF04FFFFFFFFFFFFL, PageIdUtils.pageId(0xFF04FFFFFFFFFFFFL));

        assertEquals(0x0000000000000000L, PageIdUtils.pageId(0x0000000000000000L));
        assertEquals(0x1000000000000000L, PageIdUtils.pageId(0x1000000000000000L));
        assertEquals(0x0100000000000000L, PageIdUtils.pageId(0x0100000000000000L));
        assertEquals(0x8000000000000000L, PageIdUtils.pageId(0x8000000000000000L));
        assertEquals(0x0800000000000000L, PageIdUtils.pageId(0x0800000000000000L));
        assertEquals(0xFF00000000000000L, PageIdUtils.pageId(0xFF00000000000000L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomIds() throws Exception {
        Random rnd = new Random();

        for (int i = 0; i < 50_000; i++) {
            int off = rnd.nextInt(PageIdUtils.MAX_ITEMID_NUM + 1);
            int partId = rnd.nextInt(PageIdUtils.MAX_PART_ID + 1);
            int pageNum = rnd.nextInt();

            long pageId = PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, pageNum);

            String msg = "For values [offset=" + U.hexLong(off) + ", fileId=" + U.hexLong(partId) +
                ", pageNum=" + U.hexLong(pageNum) + ']';

            assertEquals(msg, pageId, PageIdUtils.pageId(pageId));
            assertEquals(msg, 0, PageIdUtils.itemId(pageId));

            long link = PageIdUtils.link(pageId, off);

            assertEquals(msg, pageId, PageIdUtils.pageId(link));
            assertEquals(msg, off, PageIdUtils.itemId(link));
            assertEquals(msg, pageId, PageIdUtils.pageId(link));
        }
    }

    @Test
    public void testBiggerThanOldMaxItemId() {
        Random rnd = new Random();
        int partId = rnd.nextInt(PageIdUtils.MAX_PART_ID + 1);
        int pageNum = rnd.nextInt();
        int itemId = 256;

        long pageId = PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, pageNum);

        long link = PageIdUtils.link(pageId, itemId);
        int actualItemId = PageIdUtils.itemId(link);

        assertEquals(actualItemId, itemId);
    }

    @Test
    public void testMaxItemId() {
        Random rnd = new Random();
        int partId = rnd.nextInt(PageIdUtils.MAX_PART_ID + 1);
        int pageNum = rnd.nextInt();
        int itemId = PageIdUtils.MAX_ITEMID_NUM;

        long pageId = PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, pageNum);

        long link = PageIdUtils.link(pageId, itemId);
        int actualItemId = PageIdUtils.itemId(link);

        assertEquals(actualItemId, itemId);
    }

    @Test(expected = AssertionError.class)
    public void testGreaterThanMaxItemId() {
        Random rnd = new Random();
        int partId = rnd.nextInt(PageIdUtils.MAX_PART_ID + 1);
        int pageNum = rnd.nextInt();
        int itemId = PageIdUtils.MAX_ITEMID_NUM + 1;

        long pageId = PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, pageNum);

        PageIdUtils.link(pageId, itemId);
    }
}
