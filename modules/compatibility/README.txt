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

Per-version JDK mapping
-----------------------

The required fork JDK is resolved by JdkForkResolver, which owns the released-version ->
JDK map and floor-looks-up the version. Currently all released versions fork under JDK 8,
JDK 11 from 2.17.0 on. The map holds the first version of each new JDK tier; to add a
tier, append one entry.

JDK 11 is expressible in the map, but the JDK 11 fork launcher is not yet wired. A JDK 11
fork requirement fails fast with a clear message rather than silently misbehaving.

If a JDK 8 fork is required but the property is unset, the test FAILS with a message
naming the version and the property to set. If the property points at
a JDK other than 8, the test also FAILS with a corrective message.

Why this module's testCompile pins release=8: the JDK 8 fork's classpath
keeps the current modules/compatibility/target/test-classes alongside the
swapped-in previous-release ignite-core test-jar. Test class files must
remain class file version 52 to be loadable by the JDK 8 launcher. See
modules/compatibility/pom.xml for the configuration.
