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

package org.apache.ignite.internal.installer;

import java.io.IOException;

/**
 *
 */
public class ModulesManager {
    /** */
    private final ModuleBuiltins builtins;

    private final FragmentsRegistry fragReg;

    private final ConfigurationManager confMgr;

    private ModulesManager(
        ModuleBuiltins builtins,
        FragmentsRegistry fragReg,
        ConfigurationManager confMgr
    ) {
        this.builtins = builtins;
        this.fragReg = fragReg;
        this.confMgr = confMgr;
    }

    public boolean addModule(String fragment) {
        fragment = builtins.maybeResolveBuiltin(fragment);

        if (!fragment.startsWith("mvn:"))
            throw new RuntimeException("Invalid module ID (module ID must have format " +
                "\"mvn:[groupId]:[artifactId]:[version]\" or be one of built-ins): " + fragment);

        fragReg.resolveFragment(fragment);

        try {
            return confMgr.addFragment(fragment);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to reconfigure Ignite", e);
        }
    }

    public boolean removeModule(String fragment) {
        fragment = builtins.maybeResolveBuiltin(fragment);

        if (!fragment.startsWith("mvn:"))
            throw new RuntimeException("Invalid module ID (module ID must have format " +
                "\"mvn:[groupId]:[artifactId]:[version]\" or be one of built-ins): " + fragment);

        try {
            return confMgr.removeFragment(fragment);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to reconfigure Ignite", e);
        }
    }

    public static class Builder {
        private ModuleBuiltins builtins = new ModuleBuiltins();
        private FragmentsRegistry fragReg;
        private ConfigurationManager confMgr;

        public Builder setBuiltins(ModuleBuiltins builtins) {
            this.builtins = builtins;

            return this;
        }

        public Builder setFragmentsRegistry(FragmentsRegistry fragReg) {
            this.fragReg = fragReg;

            return this;
        }

        public Builder setConfigurationManager(ConfigurationManager confMgr) {
            this.confMgr = confMgr;

            return this;
        }

        public ModulesManager build() {
            return new ModulesManager(builtins, fragReg, confMgr);
        }
    }
}
