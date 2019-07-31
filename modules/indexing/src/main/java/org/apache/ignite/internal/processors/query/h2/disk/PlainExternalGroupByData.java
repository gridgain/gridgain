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
package org.apache.ignite.internal.processors.query.h2.disk;

import org.h2.command.dml.GroupByData;
import org.h2.engine.Session;
import org.h2.value.ValueRow;

/**
 * TODO: Add class description.
 */
public class PlainExternalGroupByData extends GroupByData {

    public PlainExternalGroupByData(Session ses) {
        super(ses);
    }

    @Override public Object[] nextSource(ValueRow grpKey, int width) {
        return new Object[0]; // TODO: CODE: implement.
    }

    @Override public void updateCurrent(Object[] grpByExprData) {
        // TODO: CODE: implement.
    }

    @Override public long size() {
        return 0; // TODO: CODE: implement.
    }

    @Override public boolean next() {
        return false; // TODO: CODE: implement.
    }

    @Override public ValueRow groupKey() {
        return null; // TODO: CODE: implement.
    }

    @Override public Object[] groupByExprData() {
        return new Object[0]; // TODO: CODE: implement.
    }

    @Override public void cleanup() {
        // TODO: CODE: implement.
    }

    @Override public void done(int width) {
        // TODO: CODE: implement.
    }

    @Override public void reset() {
        // TODO: CODE: implement.
    }

    @Override public void remove() {
        // TODO: CODE: implement.
    }

    @Override public void onRowProcessed() {
        // TODO: CODE: implement.
    }
}
