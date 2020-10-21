/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cli.impl;

import java.io.PrintWriter;
import java.util.Arrays;
import org.apache.ignite.internal.cli.ModuleCommandSpec;
import org.apache.ignite.internal.installer.ModulesManager;

/**
 *
 */
public class AddModuleCommandImpl extends ModuleCommandSpec.AddModuleCommandSpec {
    private PrintWriter out;

    private ModulesManager modulesMgr;

    public AddModuleCommandImpl(
        ModulesManager modulesMgr,
        PrintWriter out
    ) {
        this.modulesMgr = modulesMgr;
        this.out = out;
    }

    @Override public void run() {
        out.println("Adding modules to Ignite: " + Arrays.toString(modules));

        for (String module : modules) {
            if (modulesMgr.addModule(module))
                out.println("Successfully added module '" + module + "' to Ignite configuration");
            else
                out.println("Module '" + module + "' is already present in Ignite configuration");
        }
    }
}
