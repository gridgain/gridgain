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

namespace Apache.Ignite.Core.Tests.Binary
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Binary builder self test with dynamic type registration.
    /// </summary>
    [TestFixture]
    public class BinaryBuilderSelfTestDynamicRegistration : BinaryBuilderSelfTest
    {
        /** <inheritdoc /> */
        protected override ICollection<BinaryTypeConfiguration> GetTypeConfigurations()
        {
            // The only type to be registered is TestEnumRegistered,
            // because unregistered enums are handled differently.

            return new []
            {
                new BinaryTypeConfiguration(typeof(TestEnumRegistered))
            };
        }
    }
}