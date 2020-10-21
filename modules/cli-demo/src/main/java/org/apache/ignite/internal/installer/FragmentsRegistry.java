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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class FragmentsRegistry {
    /** */
    private final PrintWriter out;

    /** */
    private final Path mavenRoot;

    /** */
    private final Map<String, FragmentDescriptor> fragments;

    private final MavenArtifactResolver rslvr;

    public static class Builder {
        private Path libsRoot;
        private PrintWriter out;
        private MavenArtifactResolver rslvr;

        public Builder setLibsRoot(Path libsRoot) {
            this.libsRoot = libsRoot;

            return this;
        }

        public Builder setOut(PrintWriter out) {
            this.out = out;

            return this;
        }

        public Builder setArtifactResolver(MavenArtifactResolver rslvr) {
            this.rslvr = rslvr;

            return this;
        }

        public FragmentsRegistry build() throws IOException {
            Map<String, FragmentDescriptor> fragments = new HashMap<>();

            Path mavenRoot = libsRoot.resolve("maven");

            if (Files.exists(mavenRoot)) {
                Map<String, Path> mavenModules = Files.walk(mavenRoot, 1)
                    .filter(path -> {
                        return path.getFileName().toString().endsWith(".resolved");
                    })
                    .collect(Collectors.toMap(
                        path -> {
                            String name = path.getFileName().toString();

                            return name.substring(0, name.length() - ".resolved".length());
                        },
                        path -> path
                    ));

                for (Map.Entry<String, Path> entry : mavenModules.entrySet()) {
                    Collection<String> artifacts = Files.readAllLines(entry.getValue());

                    fragments.put(entry.getKey(), new FragmentDescriptor(artifacts));
                }
            }

            return new FragmentsRegistry(out, mavenRoot, fragments, rslvr);
        }
    }

    public FragmentsRegistry(
        PrintWriter out,
        Path mavenRoot,
        Map<String, FragmentDescriptor> fragments,
        MavenArtifactResolver rslvr
    ) {
        this.out = out;
        this.mavenRoot = mavenRoot;
        this.fragments = fragments;
        this.rslvr = rslvr;
    }

    public Collection<String> artifacts(String fragment) {
        return fragments.getOrDefault(fragment, FragmentDescriptor.EMPTY).artifacts();
    }

    public void resolveFragment(String fragment) {
        String[] coords = fragment.split(":");

        if (coords.length != 4)
            throw new RuntimeException("Invalid module ID (module ID must have format " +
                "\"mvn:[groupId]:[artifactId]:[version]\" or be one of built-ins): " + fragment);

        if (!fragments.containsKey(fragment)) {
            out.println(fragment + " was not downloaded, will download");
            out.flush();

            try {
                Files.createDirectories(mavenRoot);

                // The fragment was not downloaded, download it first.
                ResolveResult res = rslvr.resolve(mavenRoot, coords[1], coords[2], coords[3]);

                Path tmpFile = Files.createFile(mavenRoot.resolve(fragment + ".resolved.tmp"));

                if (Files.exists(tmpFile))
                    Files.delete(tmpFile);

                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(tmpFile.toFile()))) {
                    PrintWriter outWriter = new PrintWriter(new OutputStreamWriter(out));

                    for (String dep : res.artifacts())
                        outWriter.println(dep);

                    outWriter.flush();
                }

                Path resolvedFile = Files.createFile(mavenRoot.resolve(fragment + ".resolved"));

                if (Files.exists(resolvedFile))
                    Files.delete(resolvedFile);

                Files.move(tmpFile, resolvedFile, StandardCopyOption.ATOMIC_MOVE);

                // Update the resolved artifacts.
                fragments.put(fragment, new FragmentDescriptor(res.artifacts()));
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to add module", e);
            }
        }
    }
}
