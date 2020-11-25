package org.apache.ignite.internal.util.tostring;

import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cacheobject.UserCacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cacheobject.UserCacheObjectImpl;
import org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.IgniteSystemProperties.getString;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.SensitiveDataLogging.*;
import static org.mockito.Mockito.mock;

public class SensitiveDataToStringTest extends GridCommonAbstractTest/*extends GridAbstractTest*/ {
    /** Id organization. */
    int rndInt0 = 54321;

    /** Partition. */
    int rndInt1 = 112233;

    /** Partition. */
    int rndInt2 = 334455;

    byte[] rndArray = new byte[] { (byte) 22, (byte) 111};

    /** Person name. */
    String rndString = "qwer";

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testCacheObjectImplWithSenstitive() {
        testCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.contains(object.toString())));
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testCacheObjectImplWithHashSenstitive() {
        testCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(object.hashCode()))));
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testCacheObjectImplWithoutSenstitive() {
        testCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("CacheObject")));
    }

    private void testCacheObjectImpl(BiConsumer<String, Object> checker) {
        Person person = new Person(rndInt0, rndString);

        CacheObjectImpl testObject = new CacheObjectImpl(person, null);
        checker.accept(testObject.toString(), person);

        testObject = new UserCacheObjectImpl(person, null);
        checker.accept(testObject.toString(), person);
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testKeyCacheObjectImplWithSenstitive() {
        testKeyCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.contains(object.toString())));
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testKeyCacheObjectImplWithHashSenstitive() {
        testKeyCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(object.hashCode()))));
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testKeyCacheObjectImplWithoutSenstitive() {
        testKeyCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("KeyCacheObject")));
    }

    private void testKeyCacheObjectImpl(BiConsumer<String, Object> checker) {
        Person person = new Person(rndInt0, rndString);

        KeyCacheObjectImpl testObject = new KeyCacheObjectImpl(person, null, rndInt1);
        checker.accept(testObject.toString(), person);

        testObject = new UserKeyCacheObjectImpl(person, rndInt1);
        checker.accept(testObject.toString(), person);
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testBinaryEnumObjectImplWithSenstitive() {
        testBinaryEnumObjectImpl((strToCheck, object) -> {
            assertTrue(strToCheck, strToCheck.contains("clsName=null"));
            assertTrue(strToCheck, strToCheck.contains("ordinal=0"));
        });
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testBinaryEnumObjectImplWithHashSenstitive() {
        testBinaryEnumObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(object.hashCode()))));
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testBinaryEnumObjectImplWithoutSenstitive() {
        testBinaryEnumObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("BinaryEnum")));
    }

    private void testBinaryEnumObjectImpl(BiConsumer<String, Object> checker) {
        BinaryEnumObjectImpl testObject = new BinaryEnumObjectImpl();
        checker.accept(testObject.toString(), testObject);
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testBinaryObjectImplWithSenstitive() {
        testBinaryObjectImpl((strToCheck, object) -> {
            assertTrue(strToCheck, strToCheck.contains("arr=false"));
            assertTrue(strToCheck, strToCheck.contains("ctx=false"));
            assertTrue(strToCheck, strToCheck.contains("start=0"));
        });
    }

//    @Test
//    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
//    public void testBinaryObjectImplWithHashSenstitive() {
//        testBinaryObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(object.hashCode()))));
//    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testBinaryObjectImplWithoutSenstitive() {
        testBinaryObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("BinaryObject")));
    }

    private void testBinaryObjectImpl(BiConsumer<String, Object> checker) {
        BinaryObjectImpl testObject = new BinaryObjectImpl();
        checker.accept(testObject.toString(), testObject);
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testBinaryObjectOffheapImplWithSenstitive() {
        testBinaryObjectOffheapImpl((strToCheck, object) -> {
            assertTrue(strToCheck, strToCheck.contains("ctx=false"));
            assertTrue(strToCheck, strToCheck.contains("start=" + rndInt1));
        });
    }

//    @Test
//    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
//    public void testBinaryObjectOffheapImplWithHashSenstitive() {
//        testBinaryObjectOffheapImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(object.hashCode()))));
//    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testBinaryObjectOffheapImplWithoutSenstitive() {
        testBinaryObjectOffheapImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("BinaryObject")));
    }

    private void testBinaryObjectOffheapImpl(BiConsumer<String, Object> checker) {
        BinaryObjectOffheapImpl testObject = new BinaryObjectOffheapImpl(null, rndInt0, rndInt1, rndInt2);
        checker.accept(testObject.toString(), testObject);
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testCacheObjectByteArrayImplWithSenstitive() {
        testCacheObjectByteArrayImpl((strToCheck, object) -> {
            assertTrue(strToCheck, strToCheck.contains("arrLen=" + 2));
        });
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testCacheObjectByteArrayImplWithHashSenstitive() {
        testCacheObjectByteArrayImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(rndArray.hashCode()))));
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testCacheObjectByteArrayImplWithoutSenstitive() {
        testCacheObjectByteArrayImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("CacheObject")));
    }

    private void testCacheObjectByteArrayImpl(BiConsumer<String, Object> checker) {
        CacheObjectByteArrayImpl testObject = new CacheObjectByteArrayImpl(rndArray);
        checker.accept(testObject.toString(), testObject);

        testObject = new UserCacheObjectByteArrayImpl(rndArray);
        checker.accept(testObject.toString(), testObject);
    }

    static class Person {
        /** Id organization. */
        int orgId;

        /** Person name. */
        String name;

        /**
         * Constructor.
         *
         * @param orgId Id organization.
         * @param name Person name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(orgId, name);
        }

        @Override
        public String toString() {
            return "Person{" +
                    "orgId=" + orgId +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
