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

package org.apache.ignite.internal.processors.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.NoopConsole;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.DEACTIVATE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;

/**
 * Command line handler test with SSL and security.
 */
public class GridCommandHandlerSslWithSecurityTest extends GridCommonAbstractTest {
    /** Login. */
    private final String login = "testUsr";

    /** Password. */
    private final String pwd = "testPwd";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new TestSecurityPluginProvider(login, pwd, ALLOW_ALL, false))
            .setSslContextFactory(sslTrustedFactory("node01", "trustone"))
            .setConnectorConfiguration(
                new ConnectorConfiguration()
                    .setSslEnabled(true)
                    .setSslFactory(sslTrustedFactory("connectorServer", "trustthree"))
            );
    }

    /**
     * Verify that the command work correctly when entering passwords for
     * keystore and truststore, and that these passwords are requested only
     * once.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInputKeyTrustStorePwdOnlyOnce() throws Exception {
        IgniteEx crd = startGrid();

        crd.cluster().active(true);

        CommandHandler cmd = new CommandHandler();

        AtomicInteger keyStorePwdCnt = new AtomicInteger();
        AtomicInteger trustStorePwdCnt = new AtomicInteger();

        cmd.console = new NoopConsole() {
            /** {@inheritDoc} */
            @Override public char[] readPassword(String fmt, Object... args) {
                if (fmt.contains("keystore")) {
                    keyStorePwdCnt.incrementAndGet();

                    return keyStorePassword().toCharArray();
                }
                else if (fmt.contains("truststore")) {
                    trustStorePwdCnt.incrementAndGet();

                    return keyStorePassword().toCharArray();
                }

                return pwd.toCharArray();
            }
        };

        List<String> args = new ArrayList<>();

        args.add(DEACTIVATE.text());
        args.add("--yes");

        args.add("--user");
        args.add(login);

        args.add("--keystore");
        args.add(keyStorePath("connectorServer"));

        args.add("--truststore");
        args.add(keyStorePath("trustthree"));

        assertEquals(EXIT_CODE_OK, cmd.execute(args));
        assertEquals(1, keyStorePwdCnt.get());
        assertEquals(1, trustStorePwdCnt.get());
    }

    /**
     * Checks that control.sh script can connect to the cluster, that has SSL enabled.
     */
    @Test
    public void testConnector() throws Exception {
        IgniteEx crd = startGrid();

        crd.cluster().active(true);

        CommandHandler hnd = new CommandHandler();

        int exitCode = hnd.execute(Arrays.asList(
            "--state",
            "--user", login,
            "--password", pwd,
            "--keystore", keyStorePath("connectorClient"),
            "--keystore-password", keyStorePassword(),
            "--truststore", keyStorePath("trustthree"),
            "--truststore-password", keyStorePassword()));

        assertEquals(EXIT_CODE_OK, exitCode);
    }
}
