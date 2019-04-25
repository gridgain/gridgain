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

#ifndef _IGNITE_EXAMPLES_ADDRESS
#define _IGNITE_EXAMPLES_ADDRESS

#include "ignite/binary/binary.h"

namespace ignite
{
    namespace examples
    {
        struct Address
        {
            Address() : street(), zip(0)
            {
                // No-op.
            }

            Address(const std::string& street, int zip) :
                street(street), zip(zip)
            {
                // No-op.
            }

            std::string ToString() 
            {
                std::ostringstream oss;

                oss << "Address [street=" << street << ", zip=" << zip << "]";

                return oss.str();
            }

            std::string street;
            int zip;
        };
    }
}

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<examples::Address> : BinaryTypeDefaultAll<examples::Address>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "Address";
            }

            static void Write(BinaryWriter& writer, const examples::Address& obj)
            {
                writer.WriteString("street", obj.street);
                writer.WriteInt32("zip", obj.zip);
            }

            static void Read(BinaryReader& reader, examples::Address& dst)
            {
                dst.street = reader.ReadString("street");
                dst.zip = reader.ReadInt32("zip");
            }
        };
    }
}

#endif