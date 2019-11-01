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

namespace Apache.Ignite.Core.Tests.Client.Cluster
{
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Cluster group API tests for thin client.
    /// </summary>
    [TestFixture]
    public class ClientClusterGroupTests : ClientTestBase
    {
        /// <summary>
        /// Test thin client cluster returns the same nodes as thick one.
        /// </summary>
        [Test]
        public void TestThinAndThickClientsReturnTheSameNode()
        {
            var nodes = Ignition.GetIgnite().GetCluster().GetNodes();
            var clientNodes = Client.GetCluster().GetNodes();
            
            Assert.IsNotEmpty(nodes);
            Assert.IsNotEmpty(clientNodes);
            AssertExtensions.ReflectionEqual(nodes, clientNodes);
        }

        /// <summary>
        /// Test cluster returns the same node instance over
        /// the different calls when no topology changes have been made.
        /// </summary>
        [Test]
        public void TestClusterGroupsReturnsTheSameNodeWithSameTopology()
        {
            var clientNode = Client.GetCluster().GetNodes().SingleOrDefault();
            var clientNode2 = Client.GetCluster().GetNodes().SingleOrDefault();

            Assert.AreSame(clientNode, clientNode2);
        }

        /// <summary>
        /// Test cluster group reflects new nodes changes.
        /// </summary>
        [Test]
        public void TestClusterGroupDetectsNewTopologyChanges()
        {
            var nodes = Ignition.GetIgnite().GetCluster().GetNodes();
            var clientNodes = Client.GetCluster().GetNodes();

            var cfg = GetIgniteConfiguration();
            cfg.AutoGenerateIgniteInstanceName = true;

            using (Ignition.Start(cfg))
            {
                var nodesNew = Ignition.GetIgnite().GetCluster().GetNodes();
                var clientNodesNew = Client.GetCluster().GetNodes();

                Assert.AreEqual(2, clientNodesNew.Count);
                AssertExtensions.ReflectionEqual(nodesNew, clientNodesNew);

                var newNode = nodesNew.Single(x => x.Id == nodes.Single().Id);
                var newClientNode = clientNodesNew.Single(x => x.Id == clientNodes.Single().Id);

                AssertExtensions.ReflectionEqual(newNode, newClientNode);
            }
        }
    }
}
