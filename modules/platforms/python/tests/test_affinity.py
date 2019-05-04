# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyignite.api import *
from pyignite.datatypes import *
from pyignite.datatypes.prop_codes import *


def test_get_node_partitions(client):

    conn = client.random_node

    cache_1 = client.get_or_create_cache('test_cache_1')
    cache_2 = client.get_or_create_cache({
        PROP_NAME: 'test_cache_2',
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': ByteArray.type_name,
                'affinity_key_field_name': 'byte_affinity',
            }
        ],
    })
    cache_3 = client.get_or_create_cache('test_cache_3')
    cache_4 = client.get_or_create_cache('test_cache_4')
    cache_5 = client.get_or_create_cache('test_cache_5')
    result = cache_get_node_partitions(
        conn,
        [cache_1.cache_id, cache_2.cache_id]
    )
    assert result.status == 0, result.message

    best_conn = cache_2.get_best_node(Int.type_id)

    assert best_conn
