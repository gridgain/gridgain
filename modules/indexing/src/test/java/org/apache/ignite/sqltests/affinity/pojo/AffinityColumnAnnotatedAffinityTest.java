/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.sqltests.affinity.pojo;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * Attempt to overcome affinity field problem by marking class field with annotation: not working
 */
public class AffinityColumnAnnotatedAffinityTest extends AffinityColumnUnannotatedClassTest {

    @Override protected Class<?> getKeysCls() {
        return KeyC.class;
    }

    @Override protected Object genKey(long id) {
        return KeyC.from(id);
    }

    /**
     * Key with explicitly marked affinity key
     */
    static class KeyC {

        long id;

        @AffinityKeyMapped
        long groupId;

        static KeyC from(long id) {
            KeyC instance = new KeyC();
            instance.id = id;
            instance.groupId = id % 100;
            return instance;
        }

        @Override public String toString() {
            return "KeyC [" + "id=" + id + ", groupId=" + groupId + ']';
        }
    }

}
