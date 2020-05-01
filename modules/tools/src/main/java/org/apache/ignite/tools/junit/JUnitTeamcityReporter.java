/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.tools.junit;

import org.junit.Ignore;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 * A JUnit RunListener that produces output conforming to the Teamcity messages specification.
 *
 * Inspired by https://github.com/mbruggmann/junit-teamcity-reporter/
 */
public class JUnitTeamcityReporter extends RunListener {
    /** */
    public static volatile String suite;

    /** */
    private volatile long started;

    /** */
    @Override public void testAssumptionFailure(Failure failure) {
        System.out.println(String.format("##teamcity[testIgnored name='%s' message='%s']",
            testName(failure.getDescription()), escapeForTeamcity(failure.getMessage())));
    }

    /** */
    @Override public void testStarted(Description desc) {
        started = System.currentTimeMillis();

        System.out.println(String.format("##teamcity[testStarted name='%s' captureStandardOutput='true']", testName(desc)));
    }

    /** */
    @Override public void testFinished(Description desc) {
        System.out.println(String.format("##teamcity[testFinished name='%s' duration='%d']",
            testName(desc), System.currentTimeMillis() - started));
    }

    /** */
    @Override public void testFailure(Failure failure) {
        System.out.println(String.format("##teamcity[testFailed name='%s' message='%s' details='%s']",
            testName(failure.getDescription()),
            escapeForTeamcity(failure.getException() == null ? "null" : failure.getException().getMessage()),
            escapeForTeamcity(X.getFullStackTrace(failure.getException()))));
    }

    /** */
    @Override public void testIgnored(Description desc) {
        Ignore annotation = desc.getAnnotation(Ignore.class);

        System.out.println(String.format("##teamcity[testIgnored name='%s' message='%s']", testName(desc),
            escapeForTeamcity(annotation == null ? null : annotation.value())));
    }

    /** */
    private String testName(final Description desc) {
        return escapeForTeamcity((suite != null ? suite : desc.getClassName()) + ": " +
            desc.getClassName() + "." + desc.getMethodName());
    }

    /** */
    private String escapeForTeamcity(String msg) {
        return (msg == null ? "null" : msg)
            .replace("|", "||")
            .replace("\r", "|r")
            .replace("\n", "|n")
            .replace("'", "|'")
            .replace("[", "|[")
            .replace("]", "|]");
    }
}

