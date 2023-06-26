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

   * 2963c759 Make PREFIX and SUFFIX of JSON compatible with literal grammar
   * bdff104a Parse national compound character literals too
   * 9f1e68d8 Change JSON literals to JSON 'JSON text'
   * 4b648e28 Feature F271: Compound character literals
   * 0f27fe5b Refactor some loops in Parser
   * 0982de3c Extract Parser.readHexNumber() and inline readHexDecimal()
   * 4fe18409 Restore possibility to use L after hex numbers
   * a4e6950d Parse 0x literals as binary string is MSSQLServer and MySQL compatibility modes
   * 12a751d1 Throw sane exceptions for empty or invalid hex numbers
   * fdbfd056 Hex numbers may have odd number of characters
   * 8420320f SQL T023: Compound binary literals
   * c311da19 SQL T024: Spaces in binary literals
   * 564116cb Add binary variant of SUBSTRING function
   * e1335a56 Add binary concatenation operation
   * aae9db45 Add standard array concatenation operation
   * bc2ed943 Optimize ValueJson construction from numeric values
   * 8ea574f9 Fix bad condition in ValueJson.getInternal(byte[])
   * 401be65a Fix a typo and wrap long lines
   * 944c16c3 Correct INFORMATION_SCHEMA.TYPE_INFO for some data types
   * f2b2ba2d Improve usability of JSON parse() methods
   * 702c9680 Change internal representation of JSON to byte[]
   * 05b9b99f Do not generate STRINGDECODE for JSON literals
   * 1e1d659c Add JSONByteArrayTarget
   * b9ec1764 Add JSONBytesSource
   * 6c727764 Remove useless condition from implementations of JSONTarget.getResult()
   * d24fc255 Do not move JSON parsing position backward and extract a method
   * 332ab971 Fix possible unexpected SIOOBE in JSONStringSource
   * f2bc2144 Minor optimization of JSONStringSource.parseNumber()
   * c31d978a Extract JSONStringSource.readKeyword1()
   * 7fb70cd3 Fix conversion from row value to result set
   * 80407ed6 Do not write default ON NULL clause to SQL
   * b9645fb2 Change BNF of JSON_OBJECTAGG due to problem with autocompletion
   * 831ad72a Improve grammar of JSON_OBJECTAGG
   * 78bacdd3 Add JSON_OBJECTAGG and JSON_ARRAYAGG aggregate functions
   * 93a10c88 Make ABSENT ON NULL default for JSON_ARRAY for compatibility
   * dc53d831 Accept and optimize nested data format clauses
   * 6fcb9e3e Fix result of multi-column subquery with invisible columns
   * d4269422 Add JSON_ARRAY variant with a subquery
   * 5669e42b Cast VARCHAR to JSON properly and require FORMAT JSON in literals
   * 515ba385 Add FORMAT JSON examples
   * d75ee29d Add support for FORMAT JSON in expressions
   * 6dfbdd6b Allow unquoted ABSENT and WITHOUT columns inside of JSON functions
   * baea4a5d Add JSON_ARRAY function
   * 13eea1cd Add JSON_OBJECT support to Function.getSQL()
   * fba72663 Add JSON_OBJECT function
   * 9fd7ca9e Adjust cost of IsJsonPredicate
   * 76b244c1 Add IS JSON predicate
   * 1183d009 Allow to set SRID in cast from JSON to GEOMETRY
   * d1ce5a98 Fix commit b714d829fe97a30db168fa654c5d0f19e1e5db59
   * d2c3a531 Implement conversion from JSON to GEOMETRY
   * aef1c127 Add JSONValue and JSONValueTarget
   * 96623438 Add and use ByteStack.poll()
   * bca223f8 Reimplement ByteStack.peer() with default value instead of exception
   * b714d829 Do not write sign before positive exponent in JSON
   * 80db9074 Reimplement JSONStringSource without recursion
   * 4a13e02a Fix thrown exceptions
   * c90c5193 Do not parse JSON during conversion from GEOMETRY to JSON
   * 6caa2e4d Fix GeoJson output on Java 7
   * f7e82f8c Implement conversion from GEOMETRY to JSON
   * 73d3f7f1 Rename endCollection() to endObject() and invoke it after each object
   * 6718d400 Join cases in EWKTUtils.parseEWKT()
   * 9c7da67c Add type argument to Target.endCollectionItem()
   * 75951098 Use EWKTUtils.TYPES in EWKTTarget.writeHeader()
   * 8b398172 Add additional validation to JSONStringTarget
   * fef1650a Get rid of JSONTarget.valueSeparator()
   * fd1ec3af Add JSON data type to documentation
   * 71469ed8 Implement conversion from JSON to BINARY
   * f2f48d34 Implement conversion from BOOLEAN to JSON
   * 4a541fc0 Implement conversion from numeric values to JSON
   * d214151b Implement conversion from BYTES to JSON
   * e7c22984 Forbid incorrect surrogates in JSON
   * 1409ac9d Validate and normalize JSON values
   * 52c163df Fix building of documentation
   * d7e0552d JSON testing
   * ef6a5163 JSON basic storage add-ons
   * af8005fe copyright in ValueJson
   * 0718f85e JSON data type on transfer layer
   * 84a65f73 JSON data type on storage layer
   * 2a8d151f Initial JSON data type addition