/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.util.debug;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.debug.DebugUtils.LOG_DIR;
import static org.apache.ignite.internal.util.debug.DebugUtils.createFile;

public class FilesToArtifacts {
    private static final Set<File> FILES_TO_ARTIFACTS = ConcurrentHashMap.newKeySet();

    public static void addFilesToArtifacts(Stream<File> fileStream) {
        fileStream.filter(Objects::nonNull).filter(File::exists).forEach(FILES_TO_ARTIFACTS::add);
    }

    public static void saveFilesToArtifactsAsZip(IgniteLogger log) {
        List<File> files = createSortedListFiles(FILES_TO_ARTIFACTS);

        if (files.isEmpty()) {
            log.error("!!!!! saveFilesToArtifactsAsZip empty");

            return;
        }

        files.forEach(FILES_TO_ARTIFACTS::remove);

        File zipFile = createFile(LOG_DIR, "artifacts", ".zip");

        Set<String> fileNames = new HashSet<>();

        try (ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(zipFile))) {
            for (File file : files) {
                ZipEntry fileZipEntry = createFileZipEntry(file, fileNames);

                zipOutputStream.putNextEntry(fileZipEntry);

                try (FileInputStream fileInputStream = new FileInputStream(file)) {
                    U.copy(fileInputStream, zipOutputStream);
                }

                zipOutputStream.closeEntry();
            }

            zipOutputStream.flush();

            log.error("!!!!! saveFilesToArtifactsAsZip file=" + zipFile.getAbsolutePath());
        }
        catch (IOException e) {
            U.error(
                log,
                String.format("!!!!! Error when trying to write to zip: %s", zipFile.getAbsolutePath()),
                e
            );
        }
        finally {
            FILES_TO_ARTIFACTS.clear();
        }
    }

    private static ZipEntry createFileZipEntry(File file, Set<String> fileNames) {
        if (fileNames.add(file.getName()))
            return new ZipEntry(file.getName());

        String fileName = file.getName();

        do {
            file = file.getParentFile();

            String parentFileName = file == null ? Integer.toString(Math.abs(ThreadLocalRandom.current().nextInt())) : file.getName();

            fileName = parentFileName + "/" + fileName;
        }
        while (!fileNames.add(fileName));

        return new ZipEntry(fileName);
    }

    private static List<File> createSortedListFiles(Collection<File> files) {
        List<File> res = new ArrayList<>(files);

        res.sort(File::compareTo);

        return res;
    }
}
