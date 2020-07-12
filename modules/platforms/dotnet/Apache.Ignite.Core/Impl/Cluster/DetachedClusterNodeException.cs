using System;

namespace Apache.Ignite.Core.Impl.Cluster
{
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Indicates that an operation is not available for a detached cluster node.
    /// This cluster node either isn't currently present in cluster, or semantically detached.
    /// For example nodes returned from <code>BaselineTopology.currentBaseline()</code>
    /// are always considered as semantically detached, even if they are currently present in cluster.
    /// </summary>
    public class DetachedClusterNodeException : IgniteException
    {
        public DetachedClusterNodeException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DetachedClusterNodeException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public DetachedClusterNodeException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DetachedClusterNodeException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public DetachedClusterNodeException(string message, Exception cause)
            : base(message, cause)
        {
        }

        /// <summary>
        /// Constructs an exception.
        /// </summary>
        /// <param name="info">Serialization info.</param>
        /// <param name="ctx">Streaming context.</param>
        protected DetachedClusterNodeException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
        }
    }
}
