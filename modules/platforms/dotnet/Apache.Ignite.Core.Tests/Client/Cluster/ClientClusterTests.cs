namespace Apache.Ignite.Core.Tests.Client.Cluster
{
    using NUnit.Framework;

    public class ClientClusterTests : ClientTestBase
    {
        /// <summary>
        /// Tests the builder with existing class.
        /// </summary>
        [Test]
        public void TestClusterShouldNotBeActiveByDefault()
        {
            var clientCluster = Client.GetCluster();
            Assert.IsFalse(clientCluster.IsActive());
        }
    }
}
