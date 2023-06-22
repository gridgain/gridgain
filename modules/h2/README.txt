GridGain Ignite H2 Module
-----------------------------

GridGain Ignite H2 module provides SQL engine for ignite indexing.
Based on H2 1.4.199 version codebase.

To enable H2 module when starting a standalone node, move 'optional/ignite-h2' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing h2 Module In Maven Project
------------------------------------------

If you are using Maven to manage dependencies of your project, you can add H2 module
dependency like this (replace '${ignite.version}' with actual Ignite version you are
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
            <artifactId>ignite-h2</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>


Backported features
-------------------

* JSON data type; JSON_OBJECT, JSON_ARRAY, JSON_OBJECTAGG, and JSON_ARRAYAGG functions; JSON predicate

   backported commits from v1.4.200:

   * Make PREFIX and SUFFIX of JSON compatible with literal grammar
   * Parse national compound character literals too
   * Change JSON literals to JSON 'JSON text'
   * Feature F271: Compound character literals
   * Refactor some loops in Parser
   * Extract Parser.readHexNumber() and inline readHexDecimal()
   * Restore possibility to use L after hex numbers
   * Parse 0x literals as binary string is MSSQLServer and MySQL compatibility modes
   * Throw sane exceptions for empty or invalid hex numbers
   * Hex numbers may have odd number of characters
   * SQL T023: Compound binary literals
   * SQL T024: Spaces in binary literals
   * Add binary variant of SUBSTRING function
   * Generate standard SQL for SUBSTRING function
   * Add binary concatenation operation
   * Add standard array concatenation operation
   * Optimize ValueJson construction from numeric values
   * Fix bad condition in ValueJson.getInternal(byte[])
   * Fix a typo and wrap long lines
   * Correct INFORMATION_SCHEMA.TYPE_INFO for some data types
   * Improve usability of JSON parse() methods
   * Change internal representation of JSON to byte[]
   * Do not generate STRINGDECODE for JSON literals
   * Add JSONByteArrayTarget
   * Add JSONBytesSource
   * Remove useless condition from implementations of JSONTarget.getResult()
   * Do not move JSON parsing position backward and extract a method
   * Fix possible unexpected SIOOBE in JSONStringSource
   * Minor optimization of JSONStringSource.parseNumber()
   * Extract JSONStringSource.readKeyword1()
   * Fix conversion from row value to result set
   * Do not write default ON NULL clause to SQL
   * Change BNF of JSON_OBJECTAGG due to problem with autocompletion
   * Improve grammar of JSON_OBJECTAGG
   * Add JSON_OBJECTAGG and JSON_ARRAYAGG aggregate functions
   * Make ABSENT ON NULL default for JSON_ARRAY for compatibility
   * Accept and optimize nested data format clauses
   * Fix result of multi-column subquery with invisible columns
   * Add JSON_ARRAY variant with a subquery
   * Cast VARCHAR to JSON properly and require FORMAT JSON in literals
   * Add FORMAT JSON examples
   * Add support for FORMAT JSON in expressions
   * Allow unquoted ABSENT and WITHOUT columns inside of JSON functions
   * Add JSON_ARRAY function
   * Add JSON_OBJECT support to Function.getSQL()
   * Add JSON_OBJECT function
   * Do not convert standard TRIM function to non-standard functions
   * Adjust cost of IsJsonPredicate
   * Add IS JSON predicate
   * Allow to set SRID in cast from JSON to GEOMETRY
   * Fix commit b714d829fe97a30db168fa654c5d0f19e1e5db59
   * Implement conversion from JSON to GEOMETRY
   * Add JSONValue and JSONValueTarget
   * Add and use ByteStack.poll()
   * Reimplement ByteStack.peer() with default value instead of exception
   * Do not write sign before positive exponent in JSON
   * Reimplement JSONStringSource without recursion
   * Fix thrown exceptions
   * Do not parse JSON during conversion from GEOMETRY to JSON
   * Fix GeoJson output on Java 7
   * Implement conversion from GEOMETRY to JSON
   * Rename endCollection() to endObject() and invoke it after each object
   * Join cases in EWKTUtils.parseEWKT()
   * Add type argument to Target.endCollectionItem()
   * Use EWKTUtils.TYPES in EWKTTarget.writeHeader()
   * Add additional validation to JSONStringTarget
   * Get rid of JSONTarget.valueSeparator()
   * Add JSON data type to documentation
   * Implement conversion from JSON to BINARY
   * Implement conversion from BOOLEAN to JSON
   * Implement conversion from numeric values to JSON
   * Implement conversion from BYTES to JSON
   * Forbid incorrect surrogates in JSON
   * Validate and normalize JSON values
   * Fix building of documentation
   * JSON testing
   * JSON basic storage add-ons
   * copyright in ValueJson
   * JSON data type on transfer layer
   * JSON data type on storage layer
   * Initial JSON data type addition