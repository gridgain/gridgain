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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.Path;
import org.apache.ignite.internal.cli.impl.AllCommandsImpl;
import org.apache.ignite.internal.cli.impl.CommandFactory;
import org.apache.ignite.internal.installer.ConfigurationManager;
import org.apache.ignite.internal.installer.FragmentsRegistry;
import org.apache.ignite.internal.installer.MavenArtifactResolver;
import org.apache.ignite.internal.installer.ModulesManager;
import picocli.CommandLine;

/**
 *
 */
public class CommandHandler {
    public static void main(String[] args) throws URISyntaxException, IOException {
        File home = new File(CommandHandler.class.getProtectionDomain().getCodeSource().getLocation().toURI())
            .getParentFile();

        File libsRoot = new File(home, "ignitelibs");

        if (libsRoot.isFile()) {
            System.err.println("Command handler is lauched from a wrong location: " + home.getAbsolutePath());

            System.exit(1);
        }

        if (!libsRoot.exists()) {
            if (!libsRoot.mkdirs()) {
                System.err.println("Failed to create modules directory (make sure current user is able to write to " +
                    "the current directory): " + libsRoot.getAbsolutePath());

                System.exit(1);
            }
        }

        try (PrintWriter out = new PrintWriter(System.out)) {
            Path libsRootPath = libsRoot.toPath();

            // TODO propagate out for fragments registry in interactive mode.
            FragmentsRegistry fragmentReg = new FragmentsRegistry.Builder()
                .setLibsRoot(libsRootPath)
                .setOut(out)
                .setArtifactResolver(new MavenArtifactResolver())
                .build();

            ConfigurationManager confMgr = new ConfigurationManager.Builder()
                .setFragmentsRegistry(fragmentReg)
                .setLibsRoot(libsRootPath)
                .build();

            ModulesManager mgr = new ModulesManager.Builder()
                .setFragmentsRegistry(fragmentReg)
                .setConfigurationManager(confMgr)
                .build();

            CommandFactory factory = new CommandFactory(mgr);
            factory.setWriter(out);

            new CommandLine(new AllCommandsImpl(out), factory).execute(args);

            out.flush();
        }
    }
}
