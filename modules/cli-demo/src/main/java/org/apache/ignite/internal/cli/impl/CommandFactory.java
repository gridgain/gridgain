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
import org.apache.ignite.internal.cli.AllCommandsSpec;
import org.apache.ignite.internal.cli.InteractiveCommandsSpec;
import org.apache.ignite.internal.cli.ModuleCommandSpec;
import org.apache.ignite.internal.cli.ReplCommandSpec;
import org.apache.ignite.internal.installer.ModulesManager;
import picocli.CommandLine;

/**
 *
 */
public class CommandFactory implements CommandLine.IFactory {
    private PrintWriter writer;
    private final ModulesManager modulesMgr;

    public CommandFactory(ModulesManager modulesMgr) {
        this.modulesMgr = modulesMgr;
    }

    @Override public <K> K create(Class<K> aClass) throws Exception {
        if (aClass == InteractiveCommandsSpec.class)
            return (K)new InteractiveCommandsImpl(writer);
        if (aClass == AllCommandsSpec.class)
            return (K)new AllCommandsImpl(writer);
        else if (aClass == ReplCommandSpec.class)
            return (K)new ReplCommandImpl(this);
        else if (aClass == ModuleCommandSpec.class)
            return (K)new ModuleCommandImpl(writer);
        else if (aClass == ModuleCommandSpec.AddModuleCommandSpec.class)
            return (K)new AddModuleCommandImpl(modulesMgr, writer);
        else if (aClass == ModuleCommandSpec.RemoveModuleCommandSpec.class)
            return (K)new RemoveModuleCommandImpl(modulesMgr, writer);
        else
            throw new RuntimeException("Unknown class: " + aClass);
    }

    public void setWriter(PrintWriter writer) {
        this.writer = writer;
    }
}
