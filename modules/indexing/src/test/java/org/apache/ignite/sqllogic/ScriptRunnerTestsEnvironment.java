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

package org.apache.ignite.sqllogic;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ScriptRunnerTestsEnvironment {
    /**
     * @return Test scripts root directory.
     */
    String scriptsRoot();

    /**
     * The Regular expression may be used by debug / development runs of the test scripts
     * to specify only necessary tests to run.
     *
     * @return Regular expression to filter test path to execute.
     */
    String regex() default "";

    /**
     * @return Ignite nodes count.
     */
    int nodes() default 2;

    /**
     * @return {@code true} if the cluster must be restarted for each test group (directory).
     */
    boolean restart() default false;

    /**
     * @return default timeout for a test script.
     */
    long timeout() default 30_000;
}
