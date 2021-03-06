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

package org.apache.ignite.internal.commandline;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;

/**
 * Abstract class for control.sh commands, that support verbose mode.
 */
public abstract class AbstractCommand<T> implements Command<T> {
    /** Use verbose mode or not. */
    protected boolean verbose;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log, boolean verbose) throws Exception {
        this.verbose = verbose;
        return execute(clientCfg, log);
    }
}
