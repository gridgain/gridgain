GridGain Lucene Index Module
-----------------------------

GridGain Lucene module provides capabilities to index cache context and run SQL, full text queries against
these indexes. Can be used with JDK 8+.

To enable Lucene module when starting a standalone node, move 'optional/gridgain-lucene-8' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing module In Maven Project
------------------------------------------

If you are using Maven to manage dependencies of your project, you can add Lucene module
dependency like this (replace '${gridgain.version}' with actual GridGain version you are
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
            <artifactId>ignite-lucene</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
