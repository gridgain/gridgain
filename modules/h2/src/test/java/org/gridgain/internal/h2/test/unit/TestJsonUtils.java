/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.test.unit;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

import org.gridgain.internal.h2.test.TestBase;
import org.gridgain.internal.h2.util.json.JSONStringSource;
import org.gridgain.internal.h2.util.json.JSONStringTarget;

/**
 * Tests the classes from org.h2.util.json package.
 */
public class TestJsonUtils extends TestBase {

    private static final Charset[] CHARSETS = { StandardCharsets.UTF_8, StandardCharsets.UTF_16BE,
            StandardCharsets.UTF_16LE, Charset.forName("UTF-32BE"), Charset.forName("UTF-32LE") };

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testJsonStringTargetErrorDetection();
        testSourcesAndTargets();
    }

    private void testJsonStringTargetErrorDetection() throws Exception {
        JSONStringTarget target;
        // Unexpected end of object or array
        target = new JSONStringTarget();
        try {
            target.endObject();
            fail();
        } catch (RuntimeException expected) {
        }
        target = new JSONStringTarget();
        try {
            target.endArray();
            fail();
        } catch (RuntimeException expected) {
        }
        // Unexpected member without object
        target = new JSONStringTarget();
        try {
            target.member("1");
            fail();
        } catch (RuntimeException expected) {
        }
        // Unexpected member inside array
        target = new JSONStringTarget();
        target.startArray();
        try {
            target.member("1");
            fail();
        } catch (RuntimeException expected) {
        }
        // Unexpected member without value
        target = new JSONStringTarget();
        target.startObject();
        target.member("1");
        try {
            target.member("2");
            fail();
        } catch (RuntimeException expected) {
        }
        target = new JSONStringTarget();
        target.startObject();
        target.member("1");
        try {
            target.endObject();
            fail();
        } catch (RuntimeException expected) {
        }
        // Unexpected value without member name
        testJsonStringTargetErrorDetectionAllValues(new Callable<JSONStringTarget>() {
            @Override
            public JSONStringTarget call() throws Exception {
                JSONStringTarget target = new JSONStringTarget();
                target.startObject();
                return target;
            }
        });
        // Unexpected second value
        testJsonStringTargetErrorDetectionAllValues(new Callable<JSONStringTarget>() {
            @Override
            public JSONStringTarget call() throws Exception {
                JSONStringTarget target = new JSONStringTarget();
                target.valueNull();
                return target;
            }
        });
        // No value
        target = new JSONStringTarget();
        try {
            target.getString();
            fail();
        } catch (RuntimeException expected) {
        }
        // Unclosed object
        target = new JSONStringTarget();
        target.startObject();
        try {
            target.getString();
            fail();
        } catch (RuntimeException expected) {
        }
        // Unclosed array
        target = new JSONStringTarget();
        target.startObject();
        try {
            target.getString();
            fail();
        } catch (RuntimeException expected) {
        }
        // End of array after start of object or vice versa
        target = new JSONStringTarget();
        target.startObject();
        try {
            target.endArray();
            fail();
        } catch (RuntimeException expected) {
        }
        target = new JSONStringTarget();
        target.startArray();
        try {
            target.endObject();
            fail();
        } catch (RuntimeException expected) {
        }
    }

    private void testJsonStringTargetErrorDetectionAllValues(Callable<JSONStringTarget> initializer) throws Exception {
        JSONStringTarget target;
        target = initializer.call();
        try {
            target.valueNull();
            fail();
        } catch (RuntimeException expected) {
        }
        target = initializer.call();
        try {
            target.valueFalse();
            fail();
        } catch (RuntimeException expected) {
        }
        target = initializer.call();
        try {
            target.valueTrue();
            fail();
        } catch (RuntimeException expected) {
        }
        target = initializer.call();
        try {
            target.valueNumber(BigDecimal.ONE);
            fail();
        } catch (RuntimeException expected) {
        }
        target = initializer.call();
        try {
            target.valueString("string");
            fail();
        } catch (RuntimeException expected) {
        }
    }

    private void testSourcesAndTargets() throws Exception {
        testSourcesAndTargets("1", "1");
        testSourcesAndTargets("\uFEFF0", "0");
        testSourcesAndTargets("\uFEFF-1", "-1");
        testSourcesAndTargets("1.2", "1.2");
        testSourcesAndTargets("1.2e+1", "12");
        testSourcesAndTargets("10000.0", "10000.0");
        testSourcesAndTargets("\t\r\n 1.2E-1 ", "0.12");
        testSourcesAndTargets("9.99e99", "9.99E+99");
        testSourcesAndTargets("\"\"", "\"\"");
        testSourcesAndTargets("\"\\b\\f\\t\\r\\n\\\"\\/\\\\\\u0019\\u0020\"", "\"\\b\\f\\t\\r\\n\\\"/\\\\\\u0019 \"");
        testSourcesAndTargets("{ }", "{}");
        testSourcesAndTargets("{\"a\" : 1}", "{\"a\":1}");
        testSourcesAndTargets("{\"a\" : 1, \"b\":[]}", "{\"a\":1,\"b\":[]}");
        testSourcesAndTargets("{\"a\" : 1, \"b\":[1,null, true,false,{}]}", "{\"a\":1,\"b\":[1,null,true,false,{}]}");
        testSourcesAndTargets("{\"1\" : [[[[[[[[[[11.1e-100]]]], null]]], {\n\r}]]]}",
                "{\"1\":[[[[[[[[[[1.11E-99]]]],null]]],{}]]]}");
        testSourcesAndTargets("{\"b\":false,\"a\":1,\"a\":null}", "{\"b\":false,\"a\":1,\"a\":null}");
        testSourcesAndTargets("\"\uD800\uDFFF\"", "\"\uD800\uDFFF\"");
        testSourcesAndTargets("\"\\uD800\\uDFFF\"", "\"\uD800\uDFFF\"");
        testSourcesAndTargetsError("");
        testSourcesAndTargetsError(".1");
        testSourcesAndTargetsError("1.");
        testSourcesAndTargetsError("1.1e");
        testSourcesAndTargetsError("1.1e+");
        testSourcesAndTargetsError("1.1e-");
        testSourcesAndTargetsError("\b1");
        testSourcesAndTargetsError("\"\\u");
        testSourcesAndTargetsError("\"\\u0");
        testSourcesAndTargetsError("\"\\u00");
        testSourcesAndTargetsError("\"\\u000");
        testSourcesAndTargetsError("\"\\u0000");
        testSourcesAndTargetsError("{,}");
        testSourcesAndTargetsError("{}}");
        testSourcesAndTargetsError("[]]");
        testSourcesAndTargetsError("\"\\uZZZZ\"");
        testSourcesAndTargetsError("\"\\x\"");
        testSourcesAndTargetsError("[1,");
        testSourcesAndTargetsError("{\"a\"-1}");
        testSourcesAndTargetsError("[1;2]");
        testSourcesAndTargetsError("{\"a\":1,b:2}");
        testSourcesAndTargetsError("{\"a\":1;\"b\":2}");
        testSourcesAndTargetsError("fals");
        testSourcesAndTargetsError("falsE");
        testSourcesAndTargetsError("False");
        testSourcesAndTargetsError("nul");
        testSourcesAndTargetsError("nulL");
        testSourcesAndTargetsError("Null");
        testSourcesAndTargetsError("tru");
        testSourcesAndTargetsError("truE");
        testSourcesAndTargetsError("True");
        testSourcesAndTargetsError("\"\uD800\"");
        testSourcesAndTargetsError("\"\\uD800\"");
        testSourcesAndTargetsError("\"\uDC00\"");
        testSourcesAndTargetsError("\"\\uDC00\"");
        testSourcesAndTargetsError("\"\uDBFF \"");
        testSourcesAndTargetsError("\"\\uDBFF \"");
        testSourcesAndTargetsError("\"\uDBFF\\\"");
        testSourcesAndTargetsError("\"\\uDBFF\\\"");
        testSourcesAndTargetsError("\"\uDFFF\uD800\"");
        testSourcesAndTargetsError("\"\\uDFFF\\uD800\"");
    }

    private void testSourcesAndTargets(String src, String expected) throws Exception {
        JSONStringTarget target = new JSONStringTarget();
        JSONStringSource.parse(src, target);
        assertEquals(expected, target.getString());
        for (Charset charset : CHARSETS) {
            target = new JSONStringTarget();
            JSONStringSource.parse(src.getBytes(charset), target);
            assertEquals(expected, target.getString());
        }
    }

    private void testSourcesAndTargetsError(String src) throws Exception {
        JSONStringTarget target = new JSONStringTarget();
        try {
            JSONStringSource.parse(src, target);
        } catch (IllegalArgumentException expected) {
            // Expected
            return;
        }
        fail();
    }

}
