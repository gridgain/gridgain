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

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class ConfigurationManager {
    private final FragmentsRegistry fragmentReg;

    private final Path confFile;

    private final Path classpathFile;

    private final Set<String> fragments;

    public static class Builder {
        private FragmentsRegistry fragmentsReg;
        private Path libsRoot;

        public Builder setFragmentsRegistry(FragmentsRegistry fragmentsReg) {
            this.fragmentsReg = fragmentsReg;

            return this;
        }

        public Builder setLibsRoot(Path libsRoot) {
            this.libsRoot = libsRoot;

            return this;
        }

        public ConfigurationManager build() throws IOException {
            Path confFile = libsRoot.resolve("modules.conf");
            Path classpathFile = libsRoot.resolve("classpath.conf");

            Set<String> fragments = new HashSet<>();

            if (Files.exists(confFile))
                fragments.addAll(Files.readAllLines(confFile));

            return new ConfigurationManager(fragments, confFile, classpathFile, fragmentsReg);
        }
    }

    private ConfigurationManager(
        Set<String> fragments,
        Path confFile,
        Path classpathFile,
        FragmentsRegistry fragmentReg
    ) {
        this.fragments = fragments;
        this.confFile = confFile;
        this.classpathFile = classpathFile;
        this.fragmentReg = fragmentReg;
    }

    public boolean addFragment(String fragment) throws IOException {
        if (fragments.add(fragment)) {
            update();

            return true;
        }

        return false;
    }

    public boolean removeFragment(String fragment) throws IOException {
        if (fragments.remove(fragment)) {
            update();

            return true;
        }

        return false;
    }

    private void update() throws IOException {
        // 1. Drop classpath
        Files.deleteIfExists(classpathFile());

        // 2. Backup config.
        if (Files.exists(configurationFile()))
            Files.move(configurationFile(), backup(configurationFile()), StandardCopyOption.REPLACE_EXISTING);

        // 3. Write modules to the temp configuration file.
        writeModules(temp(configurationFile()));

        // 4. Move temp configuration to regular file.
        Files.move(temp(configurationFile()), configurationFile());

        // 5. Drop backup.
        Files.deleteIfExists(backup(configurationFile()));

        buildClasspath(classpathFile());
    }

    private void buildClasspath(Path file) throws IOException {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file.toFile()))) {
            Set<String> artifacts = new HashSet<>();

            for (String fragment : fragments) {
                for (String artifact : fragmentReg.artifacts(fragment))
                    artifacts.add(artifact);
            }

            PrintWriter pw = new PrintWriter(new OutputStreamWriter(out));

            for (String artifact : artifacts)
                pw.println(artifact);

            pw.flush();
        }
    }

    private void writeModules(Path file) throws IOException {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file.toFile()))) {
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(out));

            for (String fragment : fragments)
                pw.println(fragment);

            pw.flush();
        }
    }

    private Path configurationFile() {
        return confFile;
    }

    private Path classpathFile() {
        return classpathFile;
    }

    private Path backup(Path file) {
        return file.getParent().resolve(file.getFileName().toString() + ".backup");
    }

    private Path temp(Path file) {
        return file.getParent().resolve(file.getFileName().toString() + ".tmp");
    }
}
