/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt;

import java.security.Key;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Service to encrypt data using symmetric key encryption.
 */
public class SymmetricKeyEncryptionService implements EncryptionService {
    /** Secret key. */
    private Key secretKey;

    /** Cipher, to be used for encryption. */
    private Cipher encCipher;

    /** Cipher, to be used for decryption. */
    private Cipher decCipher;

    /**
     * The key used to encrypt and decrypt the data.
     *
     * @param secretKey Secret key.
     * @return {@code this} for chaining.
     */
    public SymmetricKeyEncryptionService setSecretKey(Key secretKey) {
        this.secretKey = secretKey;

        return this;
    }

    /** {@inheritDoc} */
    @Override public void init() throws IgniteException {
        if (secretKey == null)
            throw new IgniteException("Secret key was not set / was set to null.");

        encCipher = IgniteUtils.createCipher(secretKey, Cipher.ENCRYPT_MODE);
        decCipher = IgniteUtils.createCipher(secretKey, Cipher.DECRYPT_MODE);
    }

    /** {@inheritDoc} */
    @Override public byte[] encrypt(byte[] data) {
        if (data == null)
            throw new IgniteException("Parameter [data] cannot be null");

        if (encCipher == null)
            throw new IgniteException("The init() method was not called.");

        try {
            return encCipher.doFinal(data);
        }
        catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] decrypt(byte[] data) {
        if (data == null)
            throw new IgniteException("Parameter [data] cannot be null");

        if (decCipher == null)
            throw new IgniteException("The init() method was not called.");

        try {
            return decCipher.doFinal(data);
        }
        catch (BadPaddingException | IllegalBlockSizeException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SymmetricKeyEncryptionService.class, this, "super", super.toString());
    }
}
