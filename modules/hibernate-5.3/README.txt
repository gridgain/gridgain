GridGain Hibernate Module
------------------------------

GridGain Hibernate module provides Hibernate second-level cache (L2 cache) implementation based
on GridGain In-Memory Data Grid.

To enable Hibernate module when starting a standalone node, move 'optional/ignite-hibernate5' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing Hibernate Module In Maven Project
-------------------------------------------

If you are using Maven to manage dependencies of your project, you can add Hibernate module
dependency like this (replace '${ignite.version}' with actual GridGain version you are
interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.gridgain</groupId>
            <artifactId>ignite-hibernate_5.3</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

