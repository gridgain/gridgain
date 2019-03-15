# GridGain C++ SDK

<a href="https://www.gridgain.com/"><img src="https://www.gridgain.com/themes/gridgain1185/images/svg/gridgain-logo.svg?20180912" hspace="10"  width="300px"/></a><br/><br/>

The GridGain in-memory computing platform accelerates and scales out data-intensive applications across a distributed
computing architecture. It requires no rip-and-replace of your existing database and can be deployed on-premises, on a public or private cloud, or on a hybrid environment.

The [GridGain][GridGain-homepage] Community Edition (GCE) is a source-available in-memory computing platform built on Apache Ignite.
GCE includes the [Apache Ignite][apache-ignite-homepage] code base plus improvements and additional functionality developed by GridGain Systems
to enhance the performance and reliability of Apache Ignite. GCE is a hardened, high-performance version of Apache Ignite
for mission-critical in-memory computing applications.

<p align="center">
    <a href="https://www.gridgain.com/">
        <img src="https://files.readme.io/58b7901-gg_platform.png" width="600px"/>
    </a>
</p>

GridGain can function as an in-memory data grid accelerating a relational database or Hadoop or it can be deployed as
a in-memory database with transactional persistence.  The system supports SQL and ACID transactions, can be used for
streaming analytics and machine and deep learning use cases.The system can be scaled out by adding
new nodes to the cluster to support up to petabytes of data from multiple databases with automatic data rebalancing and
redundant storage across the nodes.

## GridGain and GridGain C++

GridGain C++ is built on top of GridGain. GridGain C++ is available as thick and [thin client](https://apacheignite-cpp.readme.io/docs/thin-client).

### GridGain C++ thick client
* GridGain С++ starts the JVM in the same process and communicates with it via JNI.
* C++, .NET, and Java nodes can join the same cluster, use the same caches, and interoperate using common binary protocol.
* Java compute jobs can execute on any node (Java, .NET, C++).


### GridGain C++ thin client
* Gridgain client simply establishes a socket connection to an individual GridGain node, and perform all operations through that node.
* Thin client does not start the JVM process.
* Thin client does not become part of the cluster.
* Thin client never holds any data or perform computations.


## Download GridGain CE

To download the free community edition, please visit : [Downloads](https://www.gridgain.com/resources/download)

## Getting Started

For information on how to get started with GridGain, please visit: [Getting Started][getting-started]

## Documentation

For complete technical documentation on how to use GridGain APIs, please visit: [GridGain Docs][docs]

For GridGain C++ documentation, please visit: [Gridgain C++ Docs](https://apacheignite-cpp.readme.io/docs)

[apache-ignite-homepage]: https://ignite.apache.org/
[GridGain-homepage]: https://www.gridgain.com/
[getting-started]: https://docs.gridgain.com/docs
[docs]: https://docs.gridgain.com/docs
