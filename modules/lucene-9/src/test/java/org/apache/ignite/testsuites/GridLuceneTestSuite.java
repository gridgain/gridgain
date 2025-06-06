/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneDirectoryTest;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneTextIndex;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Suite with tests for {@link GridLuceneTextIndex}.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        GridLuceneDirectoryTest.class
})
public class GridLuceneTestSuite {
}