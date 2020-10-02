GridGain ML Examples
======================

This folder contains code examples for GridGain ML functionality.

Examples are shipped as a separate Maven project, so to start running you simply need
to import provided `pom.xml` file into your favourite IDE.

The examples folder contains he following subfolders:

- `config` - contains GridGain configuration files needed for examples.
- `src/main/java` - contains Java examples.

Starting Remote Nodes
=====================

Remote nodes for examples should always be started with special configuration file which enables P2P
class loading: `examples-ml/config/example-ignite.xml`. To run a remote node in IDE use `ExampleNodeStartup` class.
