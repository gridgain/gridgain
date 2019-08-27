namespace Apache.Ignite.Core.Client
{
    /// <summary>
    /// Defines grid projection which represents a common functionality over a group of nodes.
    /// Grid projection allows to group Ignite nodes into various subgroups to perform distributed
    /// operations on them. All ForXXX(...)' methods will create a child grid projection
    /// from existing projection. If you create a new projection from current one, then the resulting
    /// projection will include a subset of nodes from current projection.
    /// </summary>
    public interface IClientClusterGroup 
    {
        /// <summary>
        /// Creates projection for nodes containing given name and value
        /// specified in user attributes.
        /// </summary>
        /// <param name="name">Name of the attribute.</param>
        /// <param name="val">Optional attribute value to match.</param>
        /// <returns>Grid projection for nodes containing specified attribute.</returns>
        IClientClusterGroup ForAttribute(string name, string val);
        
        /// <summary>
        /// Creates projection for all nodes that have cache with specified name running 
        /// and cache distribution mode is PARTITIONED_ONLY or NEAR_PARTITIONED.
        /// </summary>
        /// <param name="name">Cache name to include into projection.</param>
        /// <returns>Projection over nodes that have specified cache running.</returns>
        IClientClusterGroup ForDataNodes(string name);

        /// <summary>
        /// Creates grid projection for nodes supporting .Net, i.e. for nodes started with Apache.Ignite.exe.
        /// </summary>
        /// <returns>Grid projection for nodes supporting .Net.</returns>
        IClientClusterGroup ForDotNet();
    }
}
