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

// ReSharper disable UnusedAutoPropertyAccessor.Local
namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests Data Manipulation Language queries related to schema.
    /// </summary>
    public class CacheDmlQueriesTestSchema
    {
        /// Table name
        private const string TBL_NAME = "T1";

        private const string SCHEMA_NAME_1 = "SCHEMA_1";
        private const string SCHEMA_NAME_2 = "SCHEMA_2";
        private const string SCHEMA_NAME_3 = "ScHeMa3";
        private const string SCHEMA_NAME_4 = "SCHEMA_4";

        private static readonly string Q_SCHEMA_NAME_3 = '"' + SCHEMA_NAME_3 + '"';

        /// <summary>
        /// Sets up test fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration());

            Ignition.Start(cfg);
        }

        /// <summary>
        /// Tears down test fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }
 
        /// <summary>
        /// Schema explicitly defined.
        /// </summary>
        [Test]
        public void TestBasicOpsExplicitPublicSchema()
        {
            ExecuteStmtsAndVerify(() => true);
        }

        /// <summary>
        /// Schema is imlicit.
        /// </summary>
        [Test]
        public void TestBasicOpsImplicitPublicSchema()
        {
            ExecuteStmtsAndVerify(() => false);
        }

        /// <summary>
        /// Schema is mixed.
        /// </summary>
        [Test]
        public void TestBasicOpsMixedPublicSchema()
        {
            int i = 0;

            ExecuteStmtsAndVerify(() => ((++i & 1) == 0));
        }

        /// <summary>
        /// Create and drop non-existing schema.
        /// </summary>
        [Test]
        public void TestCreateDropNonExistingSchema()
        {
            Assert.Throws<IgniteException>(() => 
                Sql("CREATE TABLE UNKNOWN_SCHEMA." + TBL_NAME + "(id INT PRIMARY KEY, val INT)"));

            Assert.Throws<IgniteException>(() => Sql("DROP TABLE UNKNOWN_SCHEMA." + TBL_NAME));
        }

        /// <summary>
        /// Create tables in different schemas for same cache.
        /// </summary>
        [Test]
        public void TestCreateTblsInDiffSchemasForSameCache()
        {
            const string testCache = "cache1";

            Sql("CREATE TABLE " + SCHEMA_NAME_1 + '.' + TBL_NAME
                + " (s1_key INT PRIMARY KEY, s1_val INT) WITH \"cache_name=" + testCache + "\"");

            Assert.Throws<IgniteException>(
                () => Sql("CREATE TABLE " + SCHEMA_NAME_2 + '.' + TBL_NAME
                    + " (s1_key INT PRIMARY KEY, s2_val INT) WITH \"cache_name=" + testCache + "\"")
            );

            Sql("DROP TABLE " + SCHEMA_NAME_1 + '.' + TBL_NAME);
        }

        /// <summary>
        /// Basic test with different schemas.
        /// </summary>
        [Test]
        public void TestBasicOpsDiffSchemas()
        {
            Sql("CREATE TABLE " + SCHEMA_NAME_1 + '.' + TBL_NAME + " (s1_key INT PRIMARY KEY, s1_val INT)");
            Sql("CREATE TABLE " + SCHEMA_NAME_2 + '.' + TBL_NAME + " (s2_key INT PRIMARY KEY, s2_val INT)");
            Sql("CREATE TABLE " + Q_SCHEMA_NAME_3 + '.' + TBL_NAME + " (s3_key INT PRIMARY KEY, s3_val INT)");
            Sql("CREATE TABLE " + SCHEMA_NAME_4 + '.' + TBL_NAME + " (s4_key INT PRIMARY KEY, s4_val INT)");

            Sql("INSERT INTO " + SCHEMA_NAME_1 + '.' + TBL_NAME + " (s1_key, s1_val) VALUES (1, 2)");
            Sql("INSERT INTO " + SCHEMA_NAME_2 + '.' + TBL_NAME + " (s2_key, s2_val) VALUES (1, 2)");
            Sql("INSERT INTO " + Q_SCHEMA_NAME_3 + '.' + TBL_NAME + " (s3_key, s3_val) VALUES (1, 2)");
            Sql("INSERT INTO " + SCHEMA_NAME_4 + '.' + TBL_NAME + " (s4_key, s4_val) VALUES (1, 2)");

            Sql("UPDATE " + SCHEMA_NAME_1 + '.' + TBL_NAME + " SET s1_val = 5");
            Sql("UPDATE " + SCHEMA_NAME_2 + '.' + TBL_NAME + " SET s2_val = 5");
            Sql("UPDATE " + Q_SCHEMA_NAME_3 + '.' + TBL_NAME + " SET s3_val = 5");
            Sql("UPDATE " + SCHEMA_NAME_4 + '.' + TBL_NAME + " SET s4_val = 5");

            Sql("DELETE FROM " + SCHEMA_NAME_1 + '.' + TBL_NAME);
            Sql("DELETE FROM " + SCHEMA_NAME_2 + '.' + TBL_NAME);
            Sql("DELETE FROM " + Q_SCHEMA_NAME_3 + '.' + TBL_NAME);
            Sql("DELETE FROM " + SCHEMA_NAME_4 + '.' + TBL_NAME);

            Sql("CREATE INDEX t1_idx_1 ON " + SCHEMA_NAME_1 + '.' + TBL_NAME + "(s1_val)");
            Sql("CREATE INDEX t1_idx_1 ON " + SCHEMA_NAME_2 + '.' + TBL_NAME + "(s2_val)");
            Sql("CREATE INDEX t1_idx_1 ON " + Q_SCHEMA_NAME_3 + '.' + TBL_NAME + "(s3_val)");
            Sql("CREATE INDEX t1_idx_1 ON " + SCHEMA_NAME_4 + '.' + TBL_NAME + "(s4_val)");

            Sql("SELECT * FROM " + SCHEMA_NAME_1 + '.' + TBL_NAME);
            Sql("SELECT * FROM " + SCHEMA_NAME_2 + '.' + TBL_NAME);
            Sql("SELECT * FROM " + Q_SCHEMA_NAME_3 + '.' + TBL_NAME);
            Sql("SELECT * FROM " + SCHEMA_NAME_4 + '.' + TBL_NAME);

            Sql("SELECT * FROM " + SCHEMA_NAME_1 + '.' + TBL_NAME
                + " JOIN " + SCHEMA_NAME_2 + '.' + TBL_NAME
                + " JOIN " + Q_SCHEMA_NAME_3 + '.' + TBL_NAME
                + " JOIN " + SCHEMA_NAME_4 + '.' + TBL_NAME);

            VerifyTables();

            Sql("DROP TABLE " + SCHEMA_NAME_1 + '.' + TBL_NAME);
            Sql("DROP TABLE " + SCHEMA_NAME_2 + '.' + TBL_NAME);
            Sql("DROP TABLE " + Q_SCHEMA_NAME_3 + '.' + TBL_NAME);
            Sql("DROP TABLE " + SCHEMA_NAME_4 + '.' + TBL_NAME);
        }

        /// <summary>
        /// Verify tables.
        /// </summary>
        private void VerifyTables()
        {
            Sql("SELECT SCHEMA_NAME, KEY_ALIAS FROM SYS.TABLES ORDER BY SCHEMA_NAME", res => {
                res.Equals(new List<List<object>> {
                    new List<object>{ SCHEMA_NAME_1, "S1_KEY" },
                    new List<object>{ SCHEMA_NAME_2, "S2_KEY" },
                    new List<object>{ SCHEMA_NAME_4, "S4_KEY" },
                    new List<object>{ SCHEMA_NAME_3, "S3_KEY" }
                });
            });
        }

        /// <summary>
        /// Get test table name.
        /// </summary>
        private string GetTableName(bool withSchema) {
            string prefix = "";

            if (withSchema)
                prefix += "PUBLIC.";

            return prefix + TBL_NAME;
        }

        /// <summary>
        /// Perform SQL query.
        /// </summary>
        private void Sql(string qry)
        {
            Sql(qry, null);
        }

        /// <summary>
        /// Perform SQL query.
        /// </summary>
        private void Sql(string qry, Action<IList<IList<object>>> validator)
        {
            IList<IList<object>> res = Ignition
                .GetIgnite()
                .GetOrCreateCache<int,int>("TestCache")
                .Query(new SqlFieldsQuery(qry))
                .GetAll();

            validator?.Invoke(res);

            throw new NotImplementedException();
        }
 
        /// <summary>
        /// Generate one-row result set.
        /// </summary>
        private static List<List<object>> OneRowList(params object[] vals)
        {
            return new List<List<object>>{ new List<object>{ vals } };
        }

        /// <summary>
        /// Create/insert/update/delete/drop table in PUBLIC schema.
        /// </summary>
        private void ExecuteStmtsAndVerify(Func<bool> withSchemaDecisionSup) {
            Sql("CREATE TABLE " + GetTableName(withSchemaDecisionSup()) + " (id INT PRIMARY KEY, val INT)");

            Sql("CREATE INDEX t1_idx_1 ON " + GetTableName(withSchemaDecisionSup()) + "(val)");

            Sql("INSERT INTO " + GetTableName(withSchemaDecisionSup()) + " (id, val) VALUES(1, 2)");
            Sql("SELECT * FROM " + GetTableName(withSchemaDecisionSup()), res => OneRowList(1, 2).Equals(res));

            Sql("UPDATE " + GetTableName(withSchemaDecisionSup()) + " SET val = 5");
            Sql("SELECT * FROM " + GetTableName(withSchemaDecisionSup()), res => OneRowList(1, 5).Equals(res));

            Sql("DELETE FROM " + GetTableName(withSchemaDecisionSup()) + " WHERE id = 1");
            Sql("SELECT COUNT(*) FROM " + GetTableName(withSchemaDecisionSup()), res => OneRowList(0).Equals(res));

            Sql("SELECT COUNT(*) FROM SYS.TABLES WHERE schema_name = 'PUBLIC' " +
                "AND table_name = \'" + TBL_NAME + "\'", res => OneRowList(1).Equals(res));

            Sql("DROP TABLE " + GetTableName(withSchemaDecisionSup()));
        }
    }
}
