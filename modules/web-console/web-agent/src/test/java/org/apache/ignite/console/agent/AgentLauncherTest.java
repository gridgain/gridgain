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

package org.apache.ignite.console.agent;

import org.apache.ignite.IgniteException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Agent launcher test.
 */
public class AgentLauncherTest {
    /** Environment variables. */
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    /** Expected exception. */
    @Rule
    public final ExpectedException expEx = ExpectedException.none();

    /**
     * Should decode password with specific algorithm.
     */
    @Test
    public void shouldDecodePasswordWithSpecificAlgorithm() {
        environmentVariables.set(AgentUtils.WEB_AGENT_MASTER_PASSWORD_ENV_NAME, "SECRET_PASSWORD_EVER");
        environmentVariables.set(AgentUtils.WEB_AGENT_ENCRYPT_ALGORITHM_ENV_NAME, "PBEWITHSHA1ANDDESEDE");
        AgentConfiguration cfg = AgentLauncher.parseArgs(new String[] {"-np", "ENC(oVEmRrmjEzF5fSBgRRj/lX8MzHPdU83O3cYe+wu9YVVZeCuatrPEYw==)", "-t", "token"});

        assertEquals("password-super", cfg.nodePassword());
    }

    /**
     * Should decode password with default algorithm.
     */
    @Test
    public void shouldDecodePasswordWithDefaultAlgorithm() {
        environmentVariables.set(AgentUtils.WEB_AGENT_MASTER_PASSWORD_ENV_NAME, "SECRET_PASSWORD_EVER");
        AgentConfiguration cfg = AgentLauncher.parseArgs(new String[] {"-np", "ENC(wk07vNEjg0lYhv4uq/VmdBncMRICj+bT3kcSGNNScFPW/P5Xi6uMkA==)", "-t", "token"});

        assertEquals("password-super", cfg.nodePassword());
    }

    /**
     * Should decode password from property file with default algorithm.
     */
    @Test
    public void shouldDecodePasswordFromProeprtyFileWithDefaultAlgorithm() {
        String propPath = AgentLauncherTest.class.getClassLoader().getResource("default.properties").getPath();
        environmentVariables.set(AgentUtils.WEB_AGENT_MASTER_PASSWORD_ENV_NAME, "SECRET_PASSWORD_EVER");
        AgentConfiguration cfg = AgentLauncher.parseArgs(new String[] {"-c", propPath, "-t", "token"});

        assertEquals("password-super", cfg.nodePassword());
    }

    /**
     * Should throw exception when WEB_AGENT_MASTER_PASSWORD env variable is not set.
     */
    @Test
    public void shouldThrowExceptionIfEncodeValueExistsButPasswordEnvVarIsNotSet() {
        expEx.expect(IgniteException.class);
        expEx.expectMessage("Failed to decode value, please check that WEB_AGENT_MASTER_PASSWORD env variable is set");

        AgentConfiguration cfg = AgentLauncher.parseArgs(new String[] {"-np", "ENC(wk07vNEjg0lYhv4uq/VmdBncMRICj+bT3kcSGNNScFPW/P5Xi6uMkA==)", "-t", "token"});
        cfg.nodePassword();
    }

    /**
     * Should throw exception when WEB_AGENT_MASTER_PASSWORD env variable is incorrect.
     */
    @Test
    public void shouldThrowExceptionIfIncorrectPasswordProvided() {
        expEx.expect(IgniteException.class);
        expEx.expectMessage("Failed to decode value, please check that WEB_AGENT_MASTER_PASSWORD or WEB_AGENT_ENCRYPT_ALGORITHM env variables is correct");

        environmentVariables.set(AgentUtils.WEB_AGENT_MASTER_PASSWORD_ENV_NAME, "SECRET_PASSWORD_EVEEEER");
        AgentConfiguration cfg = AgentLauncher.parseArgs(new String[] {"-np", "ENC(wk07vNEjg0lYhv4uq/VmdBncMRICj+bT3kcSGNNScFPW/P5Xi6uMkA==)", "-t", "token"});
        cfg.nodePassword();
    }

    /**
     * Should throw exception when WEB_AGENT_ENCRYPT_ALGORITHM env variable is incorrect.
     */
    @Test
    public void shouldThrowExceptionIfIncorrectAlgorithmProvided() {
        expEx.expect(IgniteException.class);
        expEx.expectMessage("Failed to decode value, please check that WEB_AGENT_MASTER_PASSWORD or WEB_AGENT_ENCRYPT_ALGORITHM env variables is correct");

        environmentVariables.set(AgentUtils.WEB_AGENT_MASTER_PASSWORD_ENV_NAME, "SECRET_PASSWORD_EVER");
        environmentVariables.set(AgentUtils.WEB_AGENT_ENCRYPT_ALGORITHM_ENV_NAME, "PBEWITHSHA1ANDDESEDE");
        AgentConfiguration cfg = AgentLauncher.parseArgs(new String[] {"-np", "ENC(wk07vNEjg0lYhv4uq/VmdBncMRICj+bT3kcSGNNScFPW/P5Xi6uMkA==)", "-t", "token"});
        cfg.nodePassword();
    }
}
