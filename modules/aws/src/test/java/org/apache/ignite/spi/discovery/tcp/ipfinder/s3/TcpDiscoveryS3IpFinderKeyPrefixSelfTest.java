/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.client.DummyS3Client;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * TcpDiscoveryS3IpFinder tests key prefix for IP finder. For information about key prefix visit:
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ListingKeysHierarchy.html"/>.
 */
public class TcpDiscoveryS3IpFinderKeyPrefixSelfTest extends TcpDiscoveryS3IpFinderAbstractSelfTest {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryS3IpFinderKeyPrefixSelfTest() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected void setAwsCredentials(TcpDiscoveryS3IpFinder finder) {
        finder.setAwsCredentials(new BasicAWSCredentials("dummy", "dummy"));
    }

    /** {@inheritDoc} */
    @Override protected void setKeyPrefix(TcpDiscoveryS3IpFinder finder) {
        finder.setKeyPrefix("/test/key/prefix");
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryS3IpFinder ipFinder() throws Exception {
        TcpDiscoveryS3IpFinder ipFinder = Mockito.spy(new TcpDiscoveryS3IpFinder());

        Mockito.doReturn(new DummyS3Client()).when(ipFinder).createAmazonS3Client();

        setAwsCredentials(ipFinder);
        setBucketName(ipFinder);
        setKeyPrefix(ipFinder);

        return ipFinder;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testIpFinder() throws Exception {
        resources.inject(finder);

        assert finder.isShared() : "Ip finder should be shared by default.";

        super.testIpFinder();
    }
}
