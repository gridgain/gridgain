/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage;

import org.apache.ignite.IgniteBinary;

/**
 *
 */
public class SchemaDescriptor {
    /** Schema version. Incremented on each schema modification. */
    private final int ver;

    /**
     * Key columns in serialization order.
     */
    private final Columns keyCols;

    /**
     * Value columns in serialization order.
     */
    private final Columns valCols;

    public SchemaDescriptor(int ver, Columns keyCols, Columns valCols) {
        this.ver = ver;
        this.keyCols = keyCols;
        this.valCols = valCols;
    }

    public int version() {
        return ver;
    }

    public boolean keyColumn(int idx) {
        return idx < keyCols.length();
    }

    public Columns columns(int col) {
        return keyColumn(col) ? keyCols : valCols;
    }

    public Column column(int idx) {
        return keyColumn(idx) ? keyCols.column(idx) : valCols.column(idx - keyCols.length());
    }

    public Columns keyColumns() {
        return keyCols;
    }

    public Columns valueColumns() {
        return valCols;
    }

    public int length() {
        return keyCols.length() + valCols.length();
    }
}
