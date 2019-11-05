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
    using NUnit.Framework;

    /// <summary>
    /// Tests Data Manipulation Language queries related to schema.
    /// </summary>
    public class CacheDmlQueriesTestSchema
    {
        /// Table name
        private const string TBL_NAME = "T1";

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
