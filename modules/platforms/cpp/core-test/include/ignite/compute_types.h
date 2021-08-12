/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

#ifndef _IGNITE_CORE_TEST_COMPUTE_TYPES
#define _IGNITE_CORE_TEST_COMPUTE_TYPES

#include <stdint.h>
#include <string>

#include <ignite/ignite_predicate.h>
#include <ignite/cluster/cluster_node.h>

namespace ignite_test
{
    /*
     * Check if cluster node contains an attribute with name provided.
     */
    class HasAttrName : public ignite::IgnitePredicate<ignite::cluster::ClusterNode>
    {
    public:
        HasAttrName(std::string name) :
            name(name)
        {
            // No-op.
        }

        bool operator()(ignite::cluster::ClusterNode& node)
        {
            std::vector<std::string> attrs = node.GetAttributes();

            return std::find(attrs.begin(), attrs.end(), name) != attrs.end();
        }

    private:
        std::string name;
    };

    /*
     * Check if cluster node contains an attribute with value provided.
     */
    class HasAttrValue : public ignite::IgnitePredicate<ignite::cluster::ClusterNode>
    {
    public:
        HasAttrValue(std::string name, std::string val) :
            name(name),
            val(val)
        {
            // No-op.
        }

        bool operator()(ignite::cluster::ClusterNode& node)
        {
            try {
                return node.GetAttribute<std::string>(name) == this->val;
            }
            catch (...) {}

            return false;
        }

    private:
        std::string name;
        std::string val;
    };
}

#endif // _IGNITE_CORE_TEST_COMPUTE_TYPES
