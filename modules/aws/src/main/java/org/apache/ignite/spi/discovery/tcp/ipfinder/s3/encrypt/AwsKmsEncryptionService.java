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

import java.util.List;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.kms.KmsMasterKey;
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider;
import com.amazonaws.regions.Region;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Provides an implementation of AWS KMS to encrypt/decrypt the data.
 */
public class AwsKmsEncryptionService implements EncryptionService {
    /** KMS Key id. */
    private String keyId;

    /** AWS Region. */
    private Region region;

    /** AWS Credentials to access the key. */
    private AWSCredentials creds;

    /** AWS Client conf. */
    private ClientConfiguration clientConf = new ClientConfiguration();

    /** Kms master key provider. */
    private KmsMasterKeyProvider prov;

    /** Aws crypto. */
    private AwsCrypto crypto;

    /**
     * Set the KMS key id used to encrypt/decrypt the data.
     *
     * @param keyId Key id.
     * @return {@code this} for chaining.
     */
    public AwsKmsEncryptionService setKeyId(String keyId) {
        this.keyId = keyId;

        return this;
    }

    /**
     * AWS region.
     *
     * @param region Region.
     * @return {@code this} for chaining.
     */
    public AwsKmsEncryptionService setRegion(Region region) {
        this.region = region;

        return this;
    }

    /**
     * AWS credentials.
     *
     * @param creds Aws Credentials.
     * @return {@code this} for chaining.
     */
    public AwsKmsEncryptionService setCredentials(AWSCredentials creds) {
        this.creds = creds;

        return this;
    }

    /**
     * AWS client configuration.
     *
     * @param clientConf Client conf.
     * @return {@code this} for chaining.
     */
    public AwsKmsEncryptionService setClientConf(ClientConfiguration clientConf) {
        this.clientConf = clientConf;

        return this;
    }

    /** {@inheritDoc} */
    @Override public void init() {
        if (creds == null || region == null || keyId == null || keyId.trim().isEmpty())
            throw new IgniteException(String.format("At-least one of the required parameters " +
                "[creds = %s, region = %s, keyId = %s] is invalid.", creds, region, keyId));

        crypto = createClient();

        prov = createKmsMasterKeyProvider();
    }

    /** {@inheritDoc} */
    @Override public byte[] encrypt(byte[] data) {
        if (crypto == null || prov == null)
            throw new IgniteException("The init() method was not called.");

        return crypto.encryptData(prov, data).getResult();
    }

    /** {@inheritDoc} */
    @Override public byte[] decrypt(byte[] data) {
        if (crypto == null || prov == null)
            throw new IgniteException("The init() method was not called.");

        CryptoResult<byte[], KmsMasterKey> decryptRes = crypto.decryptData(prov, data);

        List<String> keyIds = decryptRes.getMasterKeyIds();

        if (keyIds != null && !keyIds.contains(keyId))
            throw new IgniteException("Wrong KMS key ID!");

        return decryptRes.getResult();
    }

    /**
     * @return An instance of {@link AwsCrypto}.
     */
    AwsCrypto createClient() {
        return crypto = new AwsCrypto();
    }

    /**
     * @return An instance of {@link KmsMasterKeyProvider}.
     */
    KmsMasterKeyProvider createKmsMasterKeyProvider() {
        return new KmsMasterKeyProvider(new AWSStaticCredentialsProvider(creds), region, clientConf, keyId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AwsKmsEncryptionService.class, this, "super", super.toString());
    }
}
