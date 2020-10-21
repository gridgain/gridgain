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

package org.apache.ignite.internal.cli;

import picocli.CommandLine;

/**
 *
 */
@CommandLine.Command(name = "module", mixinStandardHelpOptions = true,
    description = {
        "Ignite module management command",
        "Add (and download) or remove Ignite modules or external dependencies"},
    subcommands = {ModuleCommandSpec.AddModuleCommandSpec.class, ModuleCommandSpec.RemoveModuleCommandSpec.class})
public abstract class ModuleCommandSpec implements Runnable {
    @CommandLine.Command(name = "add", mixinStandardHelpOptions = true,
        description = {"Add module to Ignite startup configuration",
            "Add module to Ignite startup configuration (will automatically download the module if it has not been downloaded yet)"})
    public abstract static class AddModuleCommandSpec implements Runnable {
        @CommandLine.Parameters()
        protected String[] modules;
    }

    @CommandLine.Command(name = "remove", mixinStandardHelpOptions = true,
        description = {
            "Remove module from Ignite startup configuration",
            "Remove module from Ignite startup configuration"})
    public abstract static class RemoveModuleCommandSpec implements Runnable {
        @CommandLine.Parameters()
        protected String[] modules;
    }
}
