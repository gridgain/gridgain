/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.sqltests.affinity.arbitrary;

import java.util.Iterator;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.sqltests.affinity.AbstractAffinityColumnTest;
import org.json.JSONObject;
import org.junit.Test;

public class AffinityColumnArbitraryTypeTestX extends AbstractAffinityColumnTest {

    static final String KEY_TYPE = "ACME_KEY_TYPE";
    static final String VAL_TYPE = FOO_CACHE;

    @Override protected String getKeyType() {
        return KEY_TYPE;
    }

    @Override protected String getValType() {
        return VAL_TYPE;
    }

    @Override protected Object genKey(long id) {
        // actually we're not going to use this method
        return genBinaryKey(id);
    }

    @Override protected void put(String cache, long id) {
        putBinary(cache, id);
    }

    @Override protected void createTables() {
        super.createTables();
    }

    @Test
    public void testInsert() throws Exception {
        ignite(0).getOrCreateCache("test");

        createTable(ignite(0));

        ignite(0).cache("test").query(new SqlFieldsQuery("" +
            "insert into protectioninstrument (" +
            "   SEQUENCEKEY, COMPKEY, IGNITEKEY, REPORTINGENTITYID, ContractId, INSTRUMENTID, PROTECTIONID, PROTECTIONALLOCATEDVALUE, CHARGETYPE, DELETEDPROTECTIONINSTRUMENT, REPORTINGDATE, \"YEAR\", \"MONTH\", LASTUPLOADEDDATE, PARENTFILE, SUBMISSIONDATE" +
            ") values (" +
            "   '210-15042024-042010-ProtectionInstrument_66', '210|AAPIBW|AAPPEGX8|2010|02|07', '210|AAPIBW|AAPPEGX7', '210', 150, 'AAPIBW', 'AAPPEGX8', '72420747', '23', '', '2010-02-09 04:21:42', '2010', '02', '2024-04-17 12:06:35', '210-15042024-042010.zip', '2024-04-15 04:20:10'" +
            ")"));


    }

    public static IgniteDataStreamer<BinaryObject, BinaryObject> getProtectionInstrumentStreamer(Ignite ignite) {
        IgniteDataStreamer<BinaryObject, BinaryObject> streamer = ignite.dataStreamer("SQL_PUBLIC_PROTECTIONINSTRUMENT");
        streamer.perNodeParallelOperations(10);
        streamer.autoFlushFrequency(1000);
        streamer.allowOverwrite(true);
        return streamer;
    }

    public static IgniteBiTuple<BinaryObject, BinaryObject> jsonData(Ignite ignite) {
        String key = "{\"schema\":{\"name\":\"ACME_KEY\",\"optional\":true,\"type\":\"struct\",\"fields\":[" +
            "{\"field\":\"ContractId\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"SEQUENCEKEY\",\"optional\":true,\"type\":\"string\"}" +
            "]},\"payload\":{\"contractid\":150,\"SEQUENCEKEY\":\"210-15042024-042010-ProtectionInstrument_65\"}}";
        String value = "{\"schema\":{\"name\":\"ACME_VAL\",\"optional\":true,\"type\":\"struct\",\"fields\":[" +
            "{\"field\":\"reportingdaterowkey\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"childfile\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"protectionid\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"igniteKey\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"year\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"uploadstatus\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"instrumentid\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"reportingentityid\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"reportingdate\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"protectionallocatedvalue\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"submissionDate\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"parentfile\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"month\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"compKey\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"contractid\",\"optional\":true,\"type\":\"long\"}," +
            "{\"field\":\"lastuploadeddate\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"chargetype\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"deletedprotectioninstrument\",\"optional\":true,\"type\":\"string\"}," +
            "{\"field\":\"lineNumber\",\"optional\":true,\"type\":\"string\"}" +
            "]},\"payload\":{" +
            "\"reportingdaterowkey\":\"2010|02|09\",\"childfile\":\"210-15042024-042010-ProtectionInstrument.csv\"," +
            "\"protectionid\":\"AAPPEGX8\"," +
            "\"igniteKey\":\"210|AAPIBW|AAPPEGX8\"," +
            "\"year\":\"2010\"," +
            "\"uploadstatus\":\"N\"," +
            "\"instrumentid\":\"AAPIBW\"," +
            "\"reportingentityid\":\"210\"," +
            "\"reportingdate\":\"2010-02-09 04:21:42\"," +
            "\"protectionallocatedvalue\":\"72420747\"," +
            "\"submissionDate\":\"2024-04-15 04:20:10\"," +
            "\"parentfile\":\"210-15042024-042010.zip\"," +
            "\"month\":\"02\"," +
            "\"compKey\":\"210|AAPIBW|AAPPEGX8|2010|02|09\"," +
            //"\"contractid\":150," +
            "\"lastuploadeddate\":\"2024-04-17 12:06:35\"," +
            "\"chargetype\":\"23\"," +
            "\"deletedprotectioninstrument\":\"\"," +
            "\"lineNumber\":\"210-15042024-042010-ProtectionInstrument_65\"" +
            "}}";

        JSONObject keyObject = new JSONObject(key);
        String keyType = keyObject.getJSONObject("schema").getString("name");
        JSONObject keyPayload = keyObject.getJSONObject("payload");

        /* Preparing the Key and Value in Binary Object format. */
        BinaryObjectBuilder keyBuilder = ignite.binary().builder(keyType);
        Iterator<String> data = keyPayload.keys();
        while (data.hasNext()) {
            String Key = data.next();
            keyBuilder.setField(Key, keyPayload.get(Key));
        }
        BinaryObject k = keyBuilder.build();

        JSONObject valueObject = new JSONObject(value);
        String valueType = valueObject.getJSONObject("schema").getString("name");
        JSONObject valuePayload = valueObject.getJSONObject("payload");
        Iterator<String> values = valuePayload.keys();

        BinaryObjectBuilder valueBuilder = ignite.binary().builder(valueType);
        while (values.hasNext()) {
            String iVal = values.next();
            valueBuilder.setField(iVal, valuePayload.get(iVal));
        }
        BinaryObject v = valueBuilder.build();

        return new IgniteBiTuple<>(k, v);
    }

    public static void createTable(Ignite ignite) {
        ignite.cache("test").query(new SqlFieldsQuery("CREATE TABLE ProtectionInstrument(\n" +
            "    SequenceKey VARCHAR,\n" +
            "    CompKey VARCHAR,\n" +
            "    igniteKey VARCHAR,\n" +
            "    ReportingEntityId VARCHAR,\n" +
            "    ContractId INT,\n" +
            "    InstrumentId VARCHAR,\n" +
            "    ProtectionId VARCHAR,\n" +
            "    ProtectionAllocatedValue VARCHAR,\n" +
            "    ChargeType VARCHAR,\n" +
            "    DeletedProtectionInstrument VARCHAR,\n" +
            "    ReportingDate VARCHAR,\n" +
            "    Year VARCHAR,\n" +
            "    Month VARCHAR,\n" +
            "    LastUploadedDate VARCHAR,\n" +
            "    ParentFile VARCHAR,\n" +
            "    SubmissionDate VARCHAR,\n" +
            "    PRIMARY KEY(SequenceKey, ContractId)\n" +
            ") WITH \"CACHE_NAME=SQL_PUBLIC_PROTECTIONINSTRUMENT, KEY_TYPE=ACME_KEY,\n" +
            "VALUE_TYPE=ACME_VAL, affinity_key=ContractId, TEMPLATE=PARTITIONED, BACKUPS=1, PK_INLINE_SIZE=100, AFFINITY_INDEX_INLINE_SIZE=100\";"));

    }


}
