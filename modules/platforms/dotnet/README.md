# GridGain.NET SDK

<a href="https://www.gridgain.com/"><img src="https://www.gridgain.com/themes/gridgain1185/images/svg/gridgain-logo.svg?20180912" hspace="10"  width="300px"/></a><br/><br/>

<a href="https://www.nuget.org/packages?q=Apache.Ignite"><img src="https://img.shields.io/nuget/v/Apache.Ignite.svg" /></a>

<a href="https://www.myget.org/gallery/apache-ignite-net-nightly"><img src="https://img.shields.io/myget/apache-ignite-net-nightly/vpre/Apache.Ignite.svg" /></a>

<a href="https://ci.ignite.apache.org/viewType.html?buildTypeId=IgniteTests24Java8_IgnitePlatformNet&branch_IgniteTests24Java8=<default>"><img src="http://ci.ignite.apache.org/app/rest/builds/buildType:(id:IgniteTests24Java8_IgnitePlatformNet)/statusIcon" /></a>

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

## GridGain and GridGain.NET

GridGain.NET is built on top of GridGain. GridGain.NET is available as thick and [thin client](https://apacheignite-net.readme.io/docs/thin-client).

### Thick client
* GridGain.NET starts the JVM in the same process and communicates with it via JNI.
* .NET, C++ and Java nodes can join the same cluster, use the same caches, and interoperate using common binary protocol.
* Java compute jobs can execute on any node (Java, .NET, C++).
* .NET compute jobs can only execute on .NET nodes.

### Thin client
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

For GridGain.NET documentation, please visit: [Gridgain.NET Docs](https://apacheignite-net.readme.io/docs/)

[apache-ignite-homepage]: https://ignite.apache.org/
[GridGain-homepage]: https://www.gridgain.com/
[getting-started]: https://docs.gridgain.com/docs
[docs]: https://docs.gridgain.com/docs
