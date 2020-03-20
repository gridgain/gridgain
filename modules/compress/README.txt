GridGain Data Compression Module
------------------------

Apache Ignite Compressor module provides support for data compression in Durable Memory.
It has to be enabled on per cache basis. Currently, only Linux operating system is supported.

To enable Compressor module when starting a standalone node, move 'optional/ignite-compress' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing Compressor Module In Maven Project
---------------------------------------

If you are using Maven to manage dependencies of your project, you can add Compressor module
dependency like this (replace '${gridgain.version}' with actual Ignite version you are
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
            <artifactId>ignite-compress</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
