Ignite Compatibility Tests
------------------------

Special module contains testing framework which provides methods for testing backward compatibility when upgrading to
newer version with enabled persistence.

Cross-JDK runs (driver on JDK 17, remote fork on JDK 8)
-------------------------------------------------------

Required:
  * JDK 17 as the build/test JVM (default JAVA_HOME).
  * A JDK 8 install reachable at any filesystem path.

The remote fork runs on one of only two JDKs: the current substrate JDK (JDK 17,
inherited from the driver - no configuration needed) or the legacy JDK 8 line that
pre-JDK-17 releases must run on. Point the property at a JDK 8 install:

  mvn test -pl modules/compatibility -Pcompatibility -Dtest.multijvm.java.home=/path/to/jdk8

Per-version mapping lives in JdkForkResolver.requiredJdkMajor and defaults to JDK 8
for pre-JDK-17 GridGain 8.x version. Tests against the port-JDK-17 releases line inherit
the driver's JDK instead of requiring a JDK 8 path.

If a JDK 8 fork is required but the property is unset, the test FAILS with a message
naming the version and the property to set. If the property points at
a JDK other than 8, the test also FAILS with a corrective message.
