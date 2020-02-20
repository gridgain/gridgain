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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Eviction;

    /// <summary>
    /// Defines near cache configuration.
    /// <para />
    /// Distributed cache can also be fronted by a Near cache, which is a smaller local cache that stores most 
    /// recently or most frequently accessed data. 
    /// Just like with a partitioned cache, the user can control the size of the near cache and its eviction policies. 
    /// </summary>
    public class NearCacheConfiguration
    {
        /// <summary> Initial default near cache size. </summary>
        public const int DefaultNearStartSize = 1500000 / 4;

        /// <summary>
        /// Initializes a new instance of the <see cref="NearCacheConfiguration"/> class.
        /// </summary>
        public NearCacheConfiguration()
        {
            NearStartSize = DefaultNearStartSize;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NearCacheConfiguration"/> class.
        /// </summary>
        /// <param name="enablePlatformNearCache">When true, sets <see cref="PlatformNearCacheConfiguration"/>
        /// to a default instance.</param>
        public NearCacheConfiguration(bool enablePlatformNearCache) : this()
        {
            if (enablePlatformNearCache)
            {
                PlatformNearCacheConfiguration = new PlatformNearCacheConfiguration();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NearCacheConfiguration"/> class.
        /// </summary>
        internal NearCacheConfiguration(IBinaryRawReader reader)
        {
            NearStartSize = reader.ReadInt();
            EvictionPolicy = EvictionPolicyBase.Read(reader);

            if (reader.ReadBoolean())
            {
                PlatformNearCacheConfiguration = new PlatformNearCacheConfiguration(reader);
            }
        }

        /// <summary>
        /// Writes to the specified writer.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteInt(NearStartSize);
            EvictionPolicyBase.Write(writer, EvictionPolicy);

            if (PlatformNearCacheConfiguration != null)
            {
                writer.WriteBoolean(true);
                PlatformNearCacheConfiguration.Write(writer);
            }
            else
            {
                writer.WriteBoolean(false);
            }
        }

        /// <summary>
        /// Gets or sets the eviction policy.
        /// Null value means disabled evictions.
        /// </summary>
        public IEvictionPolicy EvictionPolicy { get; set; }

        /// <summary>
        /// Gets or sets the initial cache size for near cache which will be used 
        /// to pre-create internal hash table after start.
        /// </summary>
        [DefaultValue(DefaultNearStartSize)]
        public int NearStartSize { get; set; }
        
        /// <summary>
        /// TODO
        /// </summary>
        public PlatformNearCacheConfiguration PlatformNearCacheConfiguration { get; set; }

        /// <summary>
        /// Convenience method to set <see cref="PlatformNearCacheConfiguration"/> with specified key and value types.
        /// </summary>
        /// <param name="keepBinary">Whether to enable binary mode for the platform near cache.</param>
        /// <typeparam name="TK">Key type for near cache map.</typeparam>
        /// <typeparam name="TV">Value type for near cache map.</typeparam>
        /// <returns>This instance for chaining.</returns>
        public NearCacheConfiguration EnablePlatformNearCache<TK, TV>(bool keepBinary = false)
        {
            PlatformNearCacheConfiguration = new PlatformNearCacheConfiguration
            {
                KeepBinary = keepBinary,
                KeyTypeName = typeof(TK).AssemblyQualifiedName,
                ValueTypeName = typeof(TV).AssemblyQualifiedName
            };
            
            return this;
        }
    }
}
