/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.dev.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;

import org.gridgain.internal.h2.engine.Constants;
import org.gridgain.internal.h2.security.SHA256;
import org.gridgain.internal.h2.store.fs.FileUtils;
import org.gridgain.internal.h2.util.MathUtils;
import org.gridgain.internal.h2.util.StringUtils;
import org.gridgain.internal.h2.util.Utils;

/**
 * A tool that removes passwords from an unencrypted database.
 */
public class RemovePasswords {

    /**
     * Run the tool.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws Exception {
        execute(args[0]);
    }

    private static void execute(String fileName) throws IOException {
        fileName = FileUtils.toRealPath(fileName);
        RandomAccessFile f = new RandomAccessFile(fileName, "rw");
        long length = f.length();
        MappedByteBuffer buff = f.getChannel()
                .map(MapMode.READ_WRITE, 0, length);
        byte[] data = new byte[200];
        for (int i = 0; i < length - 200; i++) {
            if (buff.get(i) != 'C' || buff.get(i + 1) != 'R' ||
                    buff.get(i + 7) != 'U' || buff.get(i + 8) != 'S') {
                continue;
            }
            Utils.position(buff, i);
            buff.get(data);
            String s = new String(data, StandardCharsets.UTF_8);
            if (!s.startsWith("CREATE USER ")) {
                continue;
            }
            int saltIndex = Utils.indexOf(s.getBytes(), "SALT ".getBytes(), 0);
            if (saltIndex < 0) {
                continue;
            }
            String userName = s.substring("CREATE USER ".length(),
                    s.indexOf("SALT ") - 1);
            if (userName.startsWith("IF NOT EXISTS ")) {
                userName = userName.substring("IF NOT EXISTS ".length());
            }
            if (userName.startsWith("\"")) {
                // TODO doesn't work for all cases ("" inside
                // user name)
                userName = userName.substring(1, userName.length() - 1);
            }
            System.out.println("User: " + userName);
            byte[] userPasswordHash = SHA256.getKeyPasswordHash(userName,
                    "".toCharArray());
            byte[] salt = MathUtils.secureRandomBytes(Constants.SALT_LEN);
            byte[] passwordHash = SHA256
                    .getHashWithSalt(userPasswordHash, salt);
            StringBuilder b = new StringBuilder();
            b.append("SALT '").append(StringUtils.convertBytesToHex(salt))
                    .append("' HASH '")
                    .append(StringUtils.convertBytesToHex(passwordHash))
                    .append('\'');
            byte[] replacement = b.toString().getBytes();
            Utils.position(buff, i + saltIndex);
            buff.put(replacement, 0, replacement.length);
        }
        f.close();
    }

}
