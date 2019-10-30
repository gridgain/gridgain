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

namespace Apache.Ignite.Core.Impl.Client.Cluster
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Projection builder that is used for remote nodes filtering.
    /// </summary>
    internal sealed class ClientClusterGroupProjection
    {
        /** */
        private const int Attribute = 1;

        /** */
        private const int ServerNode = 1;

        private readonly Dictionary<int, object> _filter;

        private ClientClusterGroupProjection(Dictionary<int, object> filter)
        {
            _filter = filter;
        }

        /// <summary>
        /// Creates a new projection instance with specified attribute.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <param name="value">Attribute value.</param>
        /// <returns>Projection instance.</returns>
        public ClientClusterGroupProjection ForAttribute(string name, string value)
        {
            var filter = new Dictionary<int, object>(_filter);
            object attributes;
            if(filter.TryGetValue(Attribute, out attributes))
            {
                ((Dictionary<string, string>)attributes)[name] = value;
            }
            else
            {
                filter[Attribute] = new Dictionary<string, string> { { name, value } };
            }
            
            return new ClientClusterGroupProjection(filter);
        }

        /// <summary>
        /// Creates a new projection with server nodes only.
        /// </summary>
        /// <returns>Projection instance.</returns>
        public ClientClusterGroupProjection ForServerNodes()
        {
            var filter = new Dictionary<int, object>(_filter);
            filter[ServerNode] = true;
            return new ClientClusterGroupProjection(filter);
        }
        
        /// <summary>
        /// Initializes an empty projection instance.
        /// </summary>
        public static ClientClusterGroupProjection Empty
        {
            get { return new ClientClusterGroupProjection(new Dictionary<int, object>()); }
        }

        public void Marshall(IBinaryRawWriter writer)
        {
            if (_filter.Count == 0)
            {
                writer.WriteBoolean(false);
                return;
            }

            writer.WriteBoolean(true);

            // parameters serialization goes here ...
        }
    }
}
