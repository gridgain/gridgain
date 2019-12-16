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

import java.util.List;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.AbstractSelfTest;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.migration.MigrationFromMongo;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test migration from MongoDB.
 */
@SpringBootTest(properties = {"migration.mongo.db.url=mongodb://localhost:27017/console"})
public class MigrationFromMongoTest extends AbstractSelfTest {
    /** */
    private static final String MONGO_DB_URL = "mongodb://localhost:27017/console";

    /** */
    private static final String TEST_SALT = "d466d9c40905a4c9a7a31836e68162e6074bb3ba13f528e63939f75cedc6158c";

    /** */
    private static final String TEST_HASH = "eb2cc8fe73acb4554b43fecd1609bfdd98fc1ffd4830bbf0fce776d8b335e2de5ddcb70" +
        "c768a683416d1bebe85ad8f84f6d91be0b952d3589ded3d3d4b44969bbf92c0b085f6d2ac4f84f21635364bda76202137a29ec61897" +
        "d1fe3b2c90cc585f097cbd26bb1212e7d9fa51d387f914591a64ada63d9646dbbaea7a1233032fc2189df930979496026554c3e80d7" +
        "46234cd016690fc96a5c6958e157b3bd29f2da59c269279757ddcf8c1d9585c7a7bf59cfe24a7c864df6b0d72b9b1a706b00a3b337d" +
        "5305923cc683a78006bee6d659b5f574228ac0570a9a666b6c426a43d4c077db2a48d7c8473240f09870dddce986560a513109379af" +
        "946c1f4bdef444a86528e83d771007dfbc378d728d77ca155bd2e0281b9ece1cd3b2cfbd05d8f591a67df2af23ff247274db81f0cec" +
        "f0917e90d931baec05c58c94488c54488886535845ab654fa7d73b384c71421de571160f33259d49e82e2f05c02efa33304a6b55088" +
        "f734408bcf3d388b8de809ddfa445e9a82bdfd3157209e2fcb2bc074ed89c320190bd68f35f6c49816e5e82f8f044a5859a6d1a46ef" +
        "e9b32502410190d44c19b74a91c826191cd5e81572d9e23c87c97555d1d7dee4f28075118a7d9903b8888e15d957970123b8c81fd8c" +
        "2b54d2a203347e48df080c89c663d25f5d932ea0cd4a40830cd287cb4fbe3c70fb6cbdb1b7714e891dfb26336b8c964a5";

    /***/
    @Autowired
    private Ignite ignite;

    /** Migration service. */
    @Autowired
    private MigrationFromMongo migration;

    /***/
    @Autowired
    private AccountsRepository accountsRepo;

    /** In-memory java based fake MongoDb server. */
    private static MongoServer mongoSrv;

    /**
     */
    @BeforeClass
    public static void setup() {
        mongoSrv = new MongoServer(new MemoryBackend());
        mongoSrv.bind("localhost", 27017);
    }

    /**
     */
    @AfterClass
    public static void tearDown() {
        mongoSrv.shutdown();
    }

    /**
     * Initialize Ignite and MongoDB.
     *
     * @param mongoClient Mongo client.
     * @return Fresh db.
     */
    private MongoDatabase initDb(MongoClient mongoClient) {
        ignite.cache("wc_accounts").clear();

        MongoDatabase db = mongoClient.getDatabase("console");
        db.drop();

        return mongoClient.getDatabase("console");
    }

    /**
     * @param db Mongo DB.
     * @param email Account email.
     * @param token Account token.
     * @param isAdmin Admin flag.
     */
    private void createAccount(MongoDatabase db, String email, String token, boolean isAdmin) {
        MongoCollection<Document> accounts = db.getCollection("accounts");

        ObjectId accId = ObjectId.get();

        Document accDoc = new Document("_id", accId)
            .append("email", email)
            .append("salt", TEST_SALT)
            .append("hash", TEST_HASH)
            .append("firstName", "Test1")
            .append("lastName", "Test2")
            .append("phone", "222-222")
            .append("company", "Test")
            .append("country", "United States")
            .append("token", token)
            .append("resetPasswordToken", "XMEfM6vlQtze95oapezn")
            .append("admin", isAdmin);

        accounts.insertOne(accDoc);

        MongoCollection<Document> spaces = db.getCollection("spaces");

        ObjectId spaceId = ObjectId.get();
        Document spaceDoc = new Document("_id", spaceId)
            .append("owner", accId)
            .append("demo", false);

        spaces.insertOne(spaceDoc);
    }

    /**
     * Check account after migration.
     *
     * @param accounts List of migrated accounts.
     * @param email Account email.
     * @param token Account token.
     * @param isAdmin Admin flag.
     */
    private void checkAccount(List<Account> accounts, String email, String token, boolean isAdmin) {
        Account acc = accounts.stream().filter(item -> email.equals(item.getEmail())).findAny().orElse(null);

        assertNotNull(acc);
        assertEquals(email, acc.getEmail());
        assertEquals("{pbkdf2}" + TEST_SALT + TEST_HASH, acc.getPassword());
        assertEquals("Test1", acc.getFirstName());
        assertEquals("Test2", acc.getLastName());
        assertEquals("222-222", acc.getPhone());
        assertEquals("Test", acc.getCompany());
        assertEquals("United States", acc.getCountry());
        assertEquals(token, acc.getToken());
        assertEquals("XMEfM6vlQtze95oapezn", acc.getResetPasswordToken());
        assertEquals(isAdmin, acc.isAdmin());
    }


    /**
     * Should migrate single admin account correctly from MongoDb to Ignite.
     *
     * See test case #1 in GG-25440.
     */
    @Test
    public void shouldMigrateSingleAdmin() {
        try (MongoClient mongoClient = MongoClients.create(MONGO_DB_URL)) {
            MongoDatabase db = initDb(mongoClient);

            createAccount(db, "admin1@test.com", "token1", true);

            migration.migrate();

            List<Account> accounts = accountsRepo.list();

            assertEquals(1, accounts.size());

            checkAccount(accounts, "admin1@test.com", "token1", true);
        }
    }

    /**
     * Should migrate one admin account and one non-admin account.
     * See test case #2 in GG-25440.
     */
    @Test
    public void shouldMigrateOneAdminAndOneNotAdmin() {
        try (MongoClient mongoClient = MongoClients.create(MONGO_DB_URL)) {
            MongoDatabase db = initDb(mongoClient);

            createAccount(db, "admin1@test.com", "token1", true);
            createAccount(db, "user1@test.com", "token2", false);

            migration.migrate();

            List<Account> accounts = accountsRepo.list();

            assertEquals(2, accounts.size());

            checkAccount(accounts, "admin1@test.com", "token1", true);
            checkAccount(accounts, "user1@test.com", "token2", false);
        }
    }

    /**
     * Should migrate three admin accounts.
     * See test case #3 in GG-25440.
     */
    @Test
    public void shouldMigrateThreeAdmin() {
        try (MongoClient mongoClient = MongoClients.create(MONGO_DB_URL)) {
            MongoDatabase db = initDb(mongoClient);

            createAccount(db, "admin1@test.com", "token1", true);
            createAccount(db, "admin2@test.com", "token2", true);
            createAccount(db, "admin3@test.com", "token3", true);

            migration.migrate();

            List<Account> accounts = accountsRepo.list();

            assertEquals(3, accounts.size());

            checkAccount(accounts, "admin1@test.com", "token1", true);
            checkAccount(accounts, "admin2@test.com", "token2", true);
            checkAccount(accounts, "admin3@test.com", "token3", true);
        }
    }

    /**
     * Should migrate three admin accounts and one non-admin account.
     * See test case #4 in GG-25440.
     */
    @Test
    public void shouldMigrateThreeAdminAndOneNotAdmin() {
        try (MongoClient mongoClient = MongoClients.create(MONGO_DB_URL)) {
            MongoDatabase db = initDb(mongoClient);

            createAccount(db, "admin1@test.com", "token1", true);
            createAccount(db, "admin2@test.com", "token2", true);
            createAccount(db, "admin3@test.com", "token3", true);
            createAccount(db, "user1@test.com", "token4", false);

            migration.migrate();

            List<Account> accounts = accountsRepo.list();

            assertEquals(4, accounts.size());

            checkAccount(accounts, "admin1@test.com", "token1", true);
            checkAccount(accounts, "admin2@test.com", "token2", true);
            checkAccount(accounts, "admin3@test.com", "token3", true);
            checkAccount(accounts, "user1@test.com", "token4", false);
        }
    }

    /**
     * Should migrate empty Mongo database.
     *
     * See test case #5 in GG-25440.
     */
    @Test
    public void shouldMigrateEmptyDb() {
        try (MongoClient mongoClient = MongoClients.create(MONGO_DB_URL)) {
            initDb(mongoClient);

            migration.migrate();

            assertEquals(0, accountsRepo.list().size());
        }
    }
}
