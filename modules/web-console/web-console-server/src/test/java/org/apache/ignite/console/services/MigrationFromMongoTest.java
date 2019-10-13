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

package org.apache.ignite.console.services;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import java.util.Date;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.console.TestGridConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.migration.MigrationFromMongo;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.apache.ignite.console.utils.TestUtils.cleanPersistenceDir;
import static org.apache.ignite.console.utils.TestUtils.stopAllGrids;
import static org.junit.Assert.assertEquals;

/**
 * Test migration from MongoDB.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestGridConfiguration.class}, properties = {"migration.mongo.db.url=mongodb://localhost:27017/console"})
public class MigrationFromMongoTest {
    /** Migration service. */
    @Autowired
    private MigrationFromMongo migration;

    /***/
    @Autowired
    private Ignite ignite;

    /** In-memory java based fake MongoDb server. */
    private static MongoServer mongoServer;

    /**
     * @throws Exception If failed.
     */
    @BeforeClass
    public static void setup() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();

        mongoServer = new MongoServer(new MemoryBackend());
        mongoServer.bind("localhost", 27017);
    }

    /**
     * @throws Exception If failed.
     */
    @AfterClass
    public static void tearDown() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();

        mongoServer.shutdown();
    }

    /**
     * Should migrate correctly from MongoDb to Ignite.
     */
    @Test
    public void shouldMigrate() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017/console");

        MongoDatabase db = mongoClient.getDatabase("console");
        MongoCollection<Document> spaces = db.getCollection("spaces");
        MongoCollection<Document> accounts = db.getCollection("accounts");

        ObjectId spaceId = ObjectId.get();
        ObjectId accId = ObjectId.get();

        Document spaceDoc = new Document("_id", spaceId)
            .append("owner", accId)
            .append("demo", false);

        spaces.insertOne(spaceDoc);

        Date now = new Date();

        Document accDoc = new Document("_id", accId)
            .append("email", "test@test.com")
            .append("salt", "d466d9c40905a4c9a7a31836e68162e6074bb3ba13f528e63939f75cedc6158c")
            .append("hash", "eb2cc8fe73acb4554b43fecd1609bfdd98fc1ffd4830bbf0fce776d8b335e2de5ddcb70c768a683416d1bebe85ad8f84f6d91be0b952d3589ded3d3d4b44969bbf92c0b085f6d2ac4f84f21635364bda76202137a29ec61897d1fe3b2c90cc585f097cbd26bb1212e7d9fa51d387f914591a64ada63d9646dbbaea7a1233032fc2189df930979496026554c3e80d746234cd016690fc96a5c6958e157b3bd29f2da59c269279757ddcf8c1d9585c7a7bf59cfe24a7c864df6b0d72b9b1a706b00a3b337d5305923cc683a78006bee6d659b5f574228ac0570a9a666b6c426a43d4c077db2a48d7c8473240f09870dddce986560a513109379af946c1f4bdef444a86528e83d771007dfbc378d728d77ca155bd2e0281b9ece1cd3b2cfbd05d8f591a67df2af23ff247274db81f0cecf0917e90d931baec05c58c94488c54488886535845ab654fa7d73b384c71421de571160f33259d49e82e2f05c02efa33304a6b55088f734408bcf3d388b8de809ddfa445e9a82bdfd3157209e2fcb2bc074ed89c320190bd68f35f6c49816e5e82f8f044a5859a6d1a46efe9b32502410190d44c19b74a91c826191cd5e81572d9e23c87c97555d1d7dee4f28075118a7d9903b8888e15d957970123b8c81fd8c2b54d2a203347e48df080c89c663d25f5d932ea0cd4a40830cd287cb4fbe3c70fb6cbdb1b7714e891dfb26336b8c964a5")
            .append("firstName", "Test1")
            .append("lastName", "Test2")
            .append("phone", "222-222")
            .append("company", "Test")
            .append("country", "United States")
            .append("token", "hzFT7347b2Frc2cXOn0W")
            .append("resetPasswordToken", "XMEfM6vlQtze95oapezn")
            .append("lastActivity", now)
            .append("lastLogin", now)
            .append("admin", true);

        accounts.insertOne(accDoc);

        migration.migrate();

        IgniteCache<Object, Object> wc_accounts = ignite.cache("wc_accounts");

        int cnt = 0;

        for (Cache.Entry<Object, Object> e : wc_accounts) {
            Object val = e.getValue();

            if (val instanceof Account) {
                cnt++;

                Account acc = (Account)val;

                assertEquals(now.getTime(), acc.getLastActivity());
                assertEquals(now.getTime(), acc.getLastLogin());
            }
        }

        assertEquals(1, cnt);
    }
}
