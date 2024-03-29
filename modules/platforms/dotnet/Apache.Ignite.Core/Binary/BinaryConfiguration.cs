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
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Binary type configuration.
    /// </summary>
    public class BinaryConfiguration
    {
        /// <summary>
        /// Default <see cref="CompactFooter"/> setting.
        /// </summary>
        public const bool DefaultCompactFooter = true;

        /// <summary>
        /// Default <see cref="KeepDeserialized"/> setting.
        /// </summary>
        public const bool DefaultKeepDeserialized = true;

        /// <summary>
        /// Default <see cref="ForceTimestamp"/> setting.
        /// </summary>
        public const bool DefaultForceTimestamp = false;

        /// <summary>
        /// Default <see cref="UnwrapNullablePrimitiveTypes"/> setting.
        /// </summary>
        public const bool DefaultUnwrapNullablePrimitiveTypes = false;

        /** Footer setting. */
        private bool? _compactFooter;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryConfiguration"/> class.
        /// </summary>
        public BinaryConfiguration()
        {
            KeepDeserialized = DefaultKeepDeserialized;
            ForceTimestamp = DefaultForceTimestamp;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryConfiguration" /> class.
        /// </summary>
        /// <param name="cfg">The binary configuration to copy.</param>
        public BinaryConfiguration(BinaryConfiguration cfg)
        {
            IgniteArgumentCheck.NotNull(cfg, "cfg");

            CopyLocalProperties(cfg);
        }

        /// <summary>
        /// Copies the local properties.
        /// </summary>
        internal void CopyLocalProperties(BinaryConfiguration cfg)
        {
            Debug.Assert(cfg != null);

            IdMapper = cfg.IdMapper;
            NameMapper = cfg.NameMapper;
            KeepDeserialized = cfg.KeepDeserialized;
            ForceTimestamp = cfg.ForceTimestamp;
            TimestampConverter = cfg.TimestampConverter;
            UnwrapNullablePrimitiveTypes = cfg.UnwrapNullablePrimitiveTypes;

            if (cfg.Serializer != null)
            {
                Serializer = cfg.Serializer;
            }

            TypeConfigurations = cfg.TypeConfigurations == null
                ? null
                : cfg.TypeConfigurations.Select(x => new BinaryTypeConfiguration(x)).ToList();

            Types = cfg.Types == null ? null : cfg.Types.ToList();

            if (cfg.CompactFooterInternal != null)
            {
                CompactFooter = cfg.CompactFooterInternal.Value;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryConfiguration"/> class.
        /// </summary>
        /// <param name="binaryTypes">Binary types to register.</param>
        public BinaryConfiguration(params Type[] binaryTypes)
        {
            TypeConfigurations = binaryTypes.Select(t => new BinaryTypeConfiguration(t)).ToList();
        }

        /// <summary>
        /// Type configurations.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<BinaryTypeConfiguration> TypeConfigurations { get; set; }

        /// <summary>
        /// Gets or sets a collection of assembly-qualified type names
        /// (the result of <see cref="Type.AssemblyQualifiedName"/>) for binarizable types.
        /// <para />
        /// Shorthand for creating <see cref="BinaryTypeConfiguration"/>.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<string> Types { get; set; }

        /// <summary>
        /// Default name mapper.
        /// </summary>
        public IBinaryNameMapper NameMapper { get; set; }

        /// <summary>
        /// Default ID mapper.
        /// </summary>
        public IBinaryIdMapper IdMapper { get; set; }

        /// <summary>
        /// Default serializer.
        /// </summary>
        public IBinarySerializer Serializer { get; set; }

        /// <summary>
        /// Gets or sets a converter between <see cref="DateTime"/> and Java Timestamp.
        /// Called from <see cref="IBinaryWriter.WriteTimestamp"/>, <see cref="IBinaryWriter.WriteTimestampArray"/>,
        /// <see cref="IBinaryReader.ReadTimestamp"/>, <see cref="IBinaryReader.ReadTimestampArray"/>.
        /// <para />
        /// See also <see cref="ForceTimestamp"/>.
        /// </summary>
        public ITimestampConverter TimestampConverter { get; set; }

        /// <summary>
        /// Default keep deserialized flag.
        /// </summary>
        [DefaultValue(DefaultKeepDeserialized)]
        public bool KeepDeserialized { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to write footers in compact form.
        /// When enabled, Ignite will not write fields metadata when serializing objects,
        /// because internally metadata is distributed inside cluster.
        /// This increases serialization performance.
        /// <para/>
        /// <b>WARNING!</b> This mode should be disabled when already serialized data can be taken from some external
        /// sources (e.g.cache store which stores data in binary form, data center replication, etc.).
        /// Otherwise binary objects without any associated metadata could could not be deserialized.
        /// </summary>
        [DefaultValue(DefaultCompactFooter)]
        public bool CompactFooter
        {
            get { return _compactFooter ?? DefaultCompactFooter; }
            set { _compactFooter = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether all DateTime keys, values and object fields
        /// should be written as a Timestamp.
        /// <para />
        /// Timestamp format is required for values used in SQL and for interoperation with other platforms.
        /// Only UTC values are supported in Timestamp format. Other values will cause an exception on write, unless <see cref="TimestampConverter"/> is provided.
        /// <para />
        /// Normally Ignite serializer uses <see cref="IBinaryWriter.WriteObject{T}"/> for DateTime fields,
        /// keys and values.
        /// This attribute changes the behavior to <see cref="IBinaryWriter.WriteTimestamp"/>.
        /// <para />
        /// See also <see cref="TimestampAttribute"/>, <see cref="BinaryReflectiveSerializer.ForceTimestamp"/>.
        /// </summary>
        [DefaultValue(DefaultForceTimestamp)]
        public bool ForceTimestamp { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether primitive nullable object fields should be unwrapped and
        /// written as underlying type, instead of using <see cref="IBinaryWriter.WriteObject{T}"/>.
        /// <para />
        /// This produces correct field type in binary metadata and is consistent with Java serializer behavior.
        /// <para />
        /// It is recommended to enable this setting, unless you need old behavior to preserve compatibility.
        /// <para />
        /// See also <see cref="BinaryReflectiveSerializer.UnwrapNullablePrimitiveTypes"/>.
        /// </summary>
        [DefaultValue(DefaultUnwrapNullablePrimitiveTypes)]
        [IgniteExperimental]
        public bool UnwrapNullablePrimitiveTypes { get; set; }

        /// <summary>
        /// Gets the compact footer internal nullable value.
        /// </summary>
        internal bool? CompactFooterInternal
        {
            get { return _compactFooter; }
        }
    }
}
