/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

namespace Apache.Ignite.Core.Services
{
    using System;
    using System.Collections;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Services;

    /// <summary>
    /// Service call context builder.
    /// </summary>
    [IgniteExperimental]
    public sealed class ServiceCallContextBuilder
    {
        /** Context attributes. */
        private readonly Hashtable _attrs = new Hashtable();
        
        /// <summary>
        /// Set string attribute.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <param name="value">Attribute value.</param>
        /// <returns>This for chaining.</returns>
        public ServiceCallContextBuilder Set(string name, string value)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(value, "value");

            _attrs[name] = value;

            return this;
        }

        /// <summary>
        /// Set binary attribute.
        /// <p/>
        /// <b>Note:</b> it is recommended to pass a copy of the array if the original can be changed later.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <param name="value">Attribute value.</param>
        /// <returns>This for chaining.</returns>
        public ServiceCallContextBuilder Set(string name, byte[] value)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(value, "value");

            _attrs[name] = value;
            
            return this;
        }

        /// <summary>
        /// Create context.
        /// </summary>
        /// <returns>Service call context.</returns>
        public IServiceCallContext Build()
        {
            if (_attrs.Count == 0)
                throw new InvalidOperationException("Cannot create an empty context.");

            return new ServiceCallContext((Hashtable)_attrs.Clone());
        }
    }
}
