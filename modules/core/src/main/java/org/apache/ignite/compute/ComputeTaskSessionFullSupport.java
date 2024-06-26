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

package org.apache.ignite.compute;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;

/**
 * Annotation for enabling task session attributes and checkpoints for tasks.
 * <p>
 * Use this annotation when planning to use checkpoints or task session attributes API to
 * distribute session attributes between jobs.
 * <p>
 * By default, attributes and checkpoints are disabled for performance reasons.
 * @see ComputeTaskSession
 * @see ComputeTaskSession#setAttribute(Object, Object)
 * @see ComputeTaskSession#setAttributes(Map)
 * @see ComputeTaskSession#addAttributeListener(ComputeTaskSessionAttributeListener, boolean)
 * @see ComputeTaskSession#saveCheckpoint(String, Object)
 * @see ComputeTaskSession#saveCheckpoint(String, Object, ComputeTaskSessionScope, long)
 * @see ComputeTaskSession#saveCheckpoint(String, Object, ComputeTaskSessionScope, long, boolean)
 * @see ComputeTaskSession#loadCheckpoint(String)
 * @see ComputeTaskSession#removeCheckpoint(String)
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ComputeTaskSessionFullSupport {
    // No-op.
}