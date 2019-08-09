namespace Apache.Ignite.Core.Configuration
{
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Custom thread pool configuration for compute tasks.
    /// See <see cref="ICompute.WithExecutor"/>.
    /// </summary>
    public class ExecutorConfiguration
    {
        private int? _size;

        /// <summary>
        /// Gets or sets the thread pool name.
        /// Can not be null and should be unique with respect to other custom executors.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the thread pool size.
        /// Defaults to <see cref="IgniteConfiguration.DefaultThreadPoolSize"/>
        /// </summary>
        public int Size
        {
            get { return _size ?? IgniteConfiguration.DefaultThreadPoolSize; }
            set { _size = value; }
        }
    }
}
