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

package org.apache.ignite.internal.commandline.walconverter;

import java.util.Arrays;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;

import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.commandline.walconverter.ProcessSensitiveData.HASH;
import static org.apache.ignite.internal.commandline.walconverter.ProcessSensitiveData.HIDE;
import static org.apache.ignite.internal.commandline.walconverter.ProcessSensitiveData.MD5;
import static org.apache.ignite.internal.commandline.walconverter.ProcessSensitiveDataUtils.md5;

/**
 * Wrapper {@link MetastoreDataRecord} for sensitive data output.
 */
class MetastoreDataRecordWrapper extends MetastoreDataRecord {
    /**
     * Constructor.
     *
     * @param metastoreRecord Instance of {@link MetastoreDataRecord}.
     * @param sensitiveData Strategy for the processing of sensitive data.
     */
    public MetastoreDataRecordWrapper(MetastoreDataRecord metastoreRecord, ProcessSensitiveData sensitiveData) {
        super(
            HIDE == sensitiveData ? "" :
                HASH == sensitiveData ? valueOf(metastoreRecord.key().hashCode()) :
                    MD5 == sensitiveData ? md5(metastoreRecord.key()) : metastoreRecord.key(),
            HIDE == sensitiveData ? new byte[0] :
                HASH == sensitiveData ? valueOf(Arrays.hashCode(metastoreRecord.value())).getBytes(UTF_8) :
                    MD5 == sensitiveData ? md5(Arrays.toString(metastoreRecord.value())).getBytes(UTF_8) :
                        metastoreRecord.value()
        );

        size(metastoreRecord.size());
        chainSize(metastoreRecord.chainSize());
        previous(metastoreRecord.previous());
        position(metastoreRecord.position());
    }
}
