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
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DebugUtils {
    private static final DateFormat DATE_TIME_FILE_FORMAT = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_S");

    static final File LOG_DIR;

    static {
        try {
            LOG_DIR = new File(U.defaultWorkDirectory(), "log");
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    static File createFile(File dir, String filePrefix, String fileSuffix) {
        File file;

        do {
            file = new File(
                dir,
                filePrefix + "_" + U.maskForFileName(DATE_TIME_FILE_FORMAT.format(new Date())) + fileSuffix
            );
        }
        while (file.exists());

        return file;
    }

    public static void dumpWalRecords(
        @Nullable WALPointer failed,
        List<IgniteBiTuple<Thread, WALRecord>> records,
        @Nullable Throwable t,
        IgniteLogger log
    ) {
        if (records.isEmpty()) {
            log.error("!!!!! dumpWalRecords empty: " + failed);

            return;
        }

        File dumpFile = createFile(LOG_DIR, "wal_records_dump", ".txt");

        try (FileOutputStream fileOutputStream = new FileOutputStream(dumpFile)) {
            fileOutputStream.write(("failed=" + failed + U.nl() + U.nl()).getBytes(UTF_8));

            for (IgniteBiTuple<Thread, WALRecord> record : records) {
                String s = record.get2().position() + "    thread=" + record.get1().getName() + "    record=" + record.get2() + U.nl();

                fileOutputStream.write(s.getBytes(UTF_8));
            }

            fileOutputStream.flush();

            log.error(String.format("!!!!! dumpWalRecords: [file=%s, failed=%s]", dumpFile, failed), t);
        } catch (IOException e) {
            e.addSuppressed(t);

            log.error(String.format("!!!!! dumpWalRecords error: [file=%s, failed=%s]", dumpFile, failed), e);
        }
    }

    public static void dumpWalSegments(FileWALPointer segment, Collection<File> walSegmentFiles, IgniteLogger log) {
        if (walSegmentFiles.isEmpty()) {
            log.error("!!!!! dumpWalSegments empty: " + segment);

            return;
        }

        List<File> dumpWalSegmentFile = new ArrayList<>();

        for (File walSegmentFile : walSegmentFiles) {
            File dumpFile = createFile(LOG_DIR, "wal_segment_dump_" + walSegmentFile.getName(), "");

            try {
                U.copy(walSegmentFile, dumpFile, false);

                dumpWalSegmentFile.add(dumpFile);
            } catch (IOException e) {
                log.error(
                    String.format(
                        "!!!!! dumpWalSegments error: [segment=%s, walSegmentFile=%s, dumpFile=%s]",
                        segment, walSegmentFile, dumpFile
                    ),
                    e
                );
            }
        }

        log.error(
            String.format(
                "!!!!! dumpWalSegments: [segment=%s, walSegmentFiles=%s, dumpFiles=%s]",
                segment, walSegmentFiles, dumpWalSegmentFile
            )
        );
    }
}
