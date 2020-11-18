/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.yardstick.cache.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AllTypesHugeCollection implements Serializable {
    /** */
    static int COLLECTION_SIZE = Integer.parseInt(
        System.getProperty("BENCHMARK_ALL_TYPES_COLLECTION_SIZE", "1000"));

    /**
     * Data Long.
     */
    private Long longCol;

    /**
     * Data double.
     */
    private double doubleCol;

    /**
     * Data String.
     */
    private String strCol;

    /**
     * Data boolean.
     */
    private boolean booleanCol;

    /**
     * Data int.
     */
    private int intCol;

    /**
     * BigDecimal
     */
    private BigDecimal bigDecimalCol;

    /**
     * Data bytes array.
     */
    private byte[] bytesCol;

    /**
     * Data bytes array.
     */
    private Short shortCol;

    /**
     * Inner types column.
     */
    private InnerTypes innerTypesCol;

    /**
     * Enum column.
     */
    private enumType enumCol;

    /**
     * Map column.
     */
    private ConcurrentMap<String, String> mapCol = new ConcurrentHashMap<>();

    /**
     * Hash set column.
     */
    private HashSet<String> hashSetCol = new HashSet<>();

    /**
     * Enum type.
     */
    private enum enumType {
        ENUMTRUE, ENUMFALSE
    }

    /**
     * @param key Key.
     */
    public AllTypesHugeCollection(Long key) {
        init(key, Long.toString(key));
    }

    /**
     * @param key Key.
     * @param str String.
     */
    public AllTypesHugeCollection(Long key, String str) {
        init(key, str);
    }

    /**
     * @param key key
     * @param str string key
     */
    private void init(Long key, String str) {
        longCol = key;

        doubleCol = Math.round(1000 * Math.log10(longCol.doubleValue()));

        bigDecimalCol = BigDecimal.valueOf(doubleCol);

        doubleCol /= 100;

        strCol = str;

        if (key % 2 == 0) {
            booleanCol = true;

            enumCol = enumType.ENUMTRUE;

            innerTypesCol = new InnerTypes();
        } else {
            booleanCol = false;

            enumCol = enumType.ENUMFALSE;

            innerTypesCol = null;
        }

        intCol = key.intValue();

        bytesCol = strCol.getBytes();

        shortCol = (short) (((1000 * key) % 50000) - 25000);

        for (Integer i = 0; i < COLLECTION_SIZE; i++)
            mapCol.put("map_key_" + (key + i), "map_value_" + (key + i));

        for (Integer i = 0; i < COLLECTION_SIZE; i++)
            hashSetCol.add("hashset_" + (key + i));
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        List<String> hashSetColSortedList = new ArrayList<>(hashSetCol);

        Collections.sort(hashSetColSortedList);

        StringBuilder mapColStr = new StringBuilder("{");

        for (String key : new TreeSet<>(mapCol.keySet()))
            mapColStr.append(key).append("=").append(mapCol.get(key)).append(", ");

        if (mapColStr.length() > 1)
            mapColStr = new StringBuilder(mapColStr.substring(0, mapColStr.length() - 2));

        mapColStr.append("}");

        String innerTypesStr = "null";

        if (innerTypesCol != null)
            innerTypesStr = innerTypesCol.toString();

        return "AllTypes=[Long=" + longCol +
                ", double=" + doubleCol +
                ", String='" + strCol +
                "', boolean=" + booleanCol +
                ", int=" + intCol +
                ", bigDecimal=" + bigDecimalCol.toString() +
                ", bytes=" + Arrays.toString(bytesCol) +
                ", short=" + shortCol +
                ", InnerTypes=" + innerTypesStr +
                ", Map=" + mapColStr +
                ", HashSet=" + hashSetColSortedList.toString() +
                ", enum=" + enumCol.toString() +
                "]";
    }


    /**
     * Non-static inner types
     */
    private class InnerTypes implements Serializable {
        /**
         * Long column.
         */
        private Long longInnerCol;

        /**
         * String column.
         */
        private String strCol;

        /**
         * Array list column.
         */
        private ArrayList<Long> arrListCol = new ArrayList<>();

        /**
         * Default constructor.
         */
        InnerTypes() {
            longInnerCol = longCol;
            strCol = Long.toString(longCol);

            for (Integer i = 0; i < COLLECTION_SIZE; i++)
                getArrListCol().add(longCol + i);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return "[Long=" + longInnerCol +
                    ", String='" + strCol + "'" +
                    ", ArrayList=" + arrListCol.toString() +
                    "]";
        }

        /**
         * Long column.
         */
        Long getLongCol() {
            return longInnerCol;
        }

        /**
         * String column.
         */
        String getStrCol() {
            return strCol;
        }

        /**
         * Array list column.
         */
        ArrayList<Long> getArrListCol() {
            return arrListCol;
        }
    }

}