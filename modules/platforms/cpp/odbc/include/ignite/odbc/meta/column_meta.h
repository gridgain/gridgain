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

#ifndef _IGNITE_ODBC_META_COLUMN_META
#define _IGNITE_ODBC_META_COLUMN_META

#include <stdint.h>
#include <string>

#include "ignite/impl/binary/binary_reader_impl.h"

#include "ignite/odbc/protocol_version.h"
#include "ignite/odbc/common_types.h"
#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        namespace meta
        {
            /**
             * Nullability type.
             */
            struct Nullability
            {
                enum Type
                {
                    NO_NULL = 0,

                    NULLABLE = 1,

                    NULLABILITY_UNKNOWN = 2
                };

                /**
                 * Convert to SQL constant.
                 *
                 * @param nullability Nullability.
                 * @return SQL constant.
                 */
                static SqlLen ToSql(int32_t nullability);
            };

            using namespace ignite::odbc;

            /**
             * Column metadata.
             */
            class ColumnMeta
            {
            public:
                /**
                 * Convert attribute ID to string containing its name.
                 * Debug function.
                 * @param type Attribute ID.
                 * @return Null-terminated string containing attribute name.
                 */
                static const char* AttrIdToString(uint16_t id);

                /**
                 * Default constructor.
                 */
                ColumnMeta() :
                    schemaName(),
                    tableName(),
                    columnName(),
                    dataType(-1),
                    precision(-1),
                    scale(-1),
                    nullability(Nullability::NULLABILITY_UNKNOWN)
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param schemaName Schema name.
                 * @param tableName Table name.
                 * @param columnName Column name.
                 * @param typeName Type name.
                 * @param dataType Data type.
                 */
                ColumnMeta(const std::string& schemaName, const std::string& tableName,
                           const std::string& columnName, int8_t dataType) :
                    schemaName(schemaName),
                    tableName(tableName),
                    columnName(columnName),
                    dataType(dataType),
                    precision(-1),
                    scale(-1),
                    nullability(Nullability::NULLABILITY_UNKNOWN)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~ColumnMeta()
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 */
                ColumnMeta(const ColumnMeta& other) :
                    schemaName(other.schemaName),
                    tableName(other.tableName),
                    columnName(other.columnName),
                    dataType(other.dataType),
                    precision(other.precision),
                    scale(other.scale),
                    nullability(other.nullability)
                {
                    // No-op.
                }

                /**
                 * Copy operator.
                 */
                ColumnMeta& operator=(const ColumnMeta& other)
                {
                    schemaName = other.schemaName;
                    tableName = other.tableName;
                    columnName = other.columnName;
                    dataType = other.dataType;
                    precision = other.precision;
                    scale = other.scale;
                    nullability = other.nullability;

                    return *this;
                }

                /**
                 * Read using reader.
                 * @param reader Reader.
                 * @param ver Server version.
                 */
                void Read(ignite::impl::binary::BinaryReaderImpl& reader, const ProtocolVersion& ver);

                /**
                 * Get schema name.
                 * @return Schema name.
                 */
                const std::string& GetSchemaName() const
                {
                    return schemaName;
                }

                /**
                 * Get table name.
                 * @return Table name.
                 */
                const std::string& GetTableName() const
                {
                    return tableName;
                }

                /**
                 * Get column name.
                 * @return Column name.
                 */
                const std::string& GetColumnName() const
                {
                    return columnName;
                }

                /**
                 * Get data type.
                 * @return Data type.
                 */
                int8_t GetDataType() const
                {
                    return dataType;
                }

                /**
                 * Get column precision.
                 * @return Column precision.
                 */
                int32_t GetPrecision() const
                {
                    return precision;
                }

                /**
                 * Get column scale.
                 * @return Column scale.
                 */
                int32_t GetScale() const
                {
                    return scale;
                }

                /**
                 * Get column nullability.
                 * @return Column nullability.
                 */
                int32_t GetNullability() const
                {
                    return nullability;
                }

                /**
                 * Try to get attribute of a string type.
                 *
                 * @param fieldId Field ID.
                 * @param value Output attribute value.
                 * @return True if the attribute supported and false otherwise.
                 */
                bool GetAttribute(uint16_t fieldId, std::string& value) const;

                /**
                 * Try to get attribute of a integer type.
                 *
                 * @param fieldId Field ID.
                 * @param value Output attribute value.
                 * @return True if the attribute supported and false otherwise.
                 */
                bool GetAttribute(uint16_t fieldId, SqlLen& value) const;

            private:
                /** Schema name. */
                std::string schemaName;

                /** Table name. */
                std::string tableName;

                /** Column name. */
                std::string columnName;

                /** Data type. */
                int8_t dataType;

                /** Column precision. */
                int32_t precision;

                /** Column scale. */
                int32_t scale;

                /** Column nullability. */
                int32_t nullability;
            };

            /** Column metadata vector alias. */
            typedef std::vector<ColumnMeta> ColumnMetaVector;

            /**
             * Read columns metadata collection.
             * @param reader Reader.
             * @param meta Collection.
             * @param ver Server protocol version.
             */
            void ReadColumnMetaVector(ignite::impl::binary::BinaryReaderImpl& reader, ColumnMetaVector& meta,
                    const ProtocolVersion& ver);
        }
    }
}

#endif //_IGNITE_ODBC_META_COLUMN_META
