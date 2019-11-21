/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.checker;

import org.apache.ignite.internal.visor.VisorDataTransferObject;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

// TODO: 21.11.19 Tmp class only for test purposes, will be subsituted with Max's class.
public class PartitionReconciliationResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    @Override protected void writeExternalData(ObjectOutput out) throws IOException {

    }

    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {

    }
}
