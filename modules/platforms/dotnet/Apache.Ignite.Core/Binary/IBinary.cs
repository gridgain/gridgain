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

namespace Apache.Ignite.Core.Binary
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Defines binary objects functionality. With binary objects you are able to:
    /// <list type="bullet">
    ///     <item>
    ///         <description>Seamlessly interoperate between Java, .NET, and C++.</description>
    ///     </item>
    ///     <item>
    ///         <description>Make any object binary with zero code change to your existing code.</description>
    ///     </item>
    ///     <item>
    ///         <description>Nest binary objects within each other.</description>
    ///     </item>
    ///     <item>
    ///         <description>Automatically handle <c>circular</c> or <c>null</c> references.</description>
    ///     </item>
    ///     <item>
    ///         <description>Automatically convert collections and maps between Java, .NET, and C++.</description>
    ///     </item>
    ///     <item>
    ///         <description>Optionally avoid deserialization of objects on the server side.</description>
    ///     </item>
    ///     <item>
    ///         <description>Avoid need to have concrete class definitions on the server side.</description>
    ///     </item>
    ///     <item>
    ///         <description>Dynamically change structure of the classes without having to restart the cluster.</description>
    ///     </item>
    ///     <item>
    ///         <description>Index into binary objects for querying purposes.</description>
    ///     </item>
    /// </list>
    /// </summary>
    public interface IBinary
    {
        /// <summary>
        /// Converts provided object to binary form.
        /// <para />
        /// Note that object's type needs to be configured in <see cref="BinaryConfiguration"/>.
        /// </summary>
        /// <param name="obj">Object to convert.</param>
        /// <returns>Converted object.</returns>
        T ToBinary<T>(object obj);

        /// <summary>
        /// Create builder for the given binary object type. Note that this
        /// type must be specified in <see cref="BinaryConfiguration"/>.
        /// </summary>
        /// <param name="type"></param>
        /// <returns>Builder.</returns>
        IBinaryObjectBuilder GetBuilder(Type type);

        /// <summary>
        /// Create builder for the given binary object type name.
        /// </summary>
        /// <param name="typeName">Type name.</param>
        /// <returns>Builder.</returns>
        IBinaryObjectBuilder GetBuilder(string typeName);

        /// <summary>
        /// Create builder over existing binary object.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns>Builder.</returns>
        IBinaryObjectBuilder GetBuilder(IBinaryObject obj);

        /// <summary>
        /// Gets type id for the given type name.
        /// </summary>
        /// <param name="typeName">Type name.</param>
        /// <returns>Type id.</returns>
        int GetTypeId(string typeName);

        /// <summary>
        /// Gets metadata for all known types.
        /// </summary>
        /// <returns>Metadata.</returns>
        ICollection<IBinaryType> GetBinaryTypes();

        /// <summary>
        /// Gets metadata for specified type id.
        /// </summary>
        /// <returns>Metadata.</returns>
        IBinaryType GetBinaryType(int typeId);

        /// <summary>
        /// Gets metadata for specified type name.
        /// </summary>
        /// <returns>Metadata.</returns>
        IBinaryType GetBinaryType(string typeName);

        /// <summary>
        /// Gets metadata for specified type.
        /// </summary>
        /// <returns>Metadata.</returns>
        IBinaryType GetBinaryType(Type type);

        /// <summary>
        /// Converts enum to a binary form.
        /// </summary>
        /// <param name="typeName">Enum type name.</param>
        /// <param name="value">Enum int value.</param>
        /// <returns>Binary representation of the specified enum value.</returns>
        IBinaryObject BuildEnum(string typeName, int value);

        /// <summary>
        /// Converts enum to a binary form.
        /// </summary>
        /// <param name="type">Enum type.</param>
        /// <param name="value">Enum int value.</param>
        /// <returns>Binary representation of the specified enum value.</returns>
        IBinaryObject BuildEnum(Type type, int value);

        /// <summary>
        /// Converts enum to a binary form.
        /// </summary>
        /// <param name="typeName">Enum type name.</param>
        /// <param name="valueName">Enum value name.</param>
        /// <returns>Binary representation of the specified enum value.</returns>
        IBinaryObject BuildEnum(string typeName, string valueName);

        /// <summary>
        /// Converts enum to a binary form.
        /// </summary>
        /// <param name="type">Enum type.</param>
        /// <param name="valueName">Enum value name.</param>
        /// <returns>Binary representation of the specified enum value.</returns>
        IBinaryObject BuildEnum(Type type, string valueName);

        /// <summary>
        /// Registers enum type.
        /// </summary>
        /// <param name="typeName">Name of the type.</param>
        /// <param name="values">Mapping of enum value names to int values.</param>
        /// <returns>Binary type for registered enum.</returns>
        IBinaryType RegisterEnum(string typeName, IEnumerable<KeyValuePair<string, int>> values);

        /// <summary>
        /// Removes (de-registers) binary type with the specified type id.
        /// <para />
        /// Throws <see cref="IgniteException"/> if the specified type id is not found or already being removed.
        /// </summary>
        /// <param name="typeId">Type id.</param>
        void RemoveBinaryType(int typeId);
    }
}
