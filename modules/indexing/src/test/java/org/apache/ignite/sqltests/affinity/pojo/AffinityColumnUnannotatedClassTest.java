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

public class AffinityColumnUnannotatedClassTest extends AbstractAffinityColumnPojoClassTest {

    @Override protected Object genKey(long id) {
        return KeyA.from(id);
    }

    @Override protected Class<?> getKeyCls() {
        return KeyA.class;
    }

    /**
     * Key without explicitly marked affinity key
     */
    public static class KeyA {

        long userId;
        long groupId;

        static KeyA from(long id) {
            KeyA instance = new KeyA();
            instance.userId = id;
            instance.groupId = id % 100;
            return instance;
        }

        @Override public String toString() {
            return "KeyA [" + "id=" + userId + ", groupId=" + groupId + ']';
        }

    }
}
