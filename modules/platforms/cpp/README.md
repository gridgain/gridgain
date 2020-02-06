# GridGain C++

<a href="https://ci.ignite.apache.org/buildConfiguration/IgniteTests24Java8_PlatformCLinux&branch=<default>"><img src="http://ci.ignite.apache.org/app/rest/builds/buildType:(id:IgniteTests24Java8_PlatformCLinux)/statusIcon" /></a>

## Getting Started

For information on how to get started with GridGain, please visit: [Getting Started][getting-started].


## What is GridGain?

GridGain, built on top of Apache Ignite, is a memory-centric distributed **database**, **caching**,
 and **processing** platform for transactional, analytical, and streaming workloads delivering in-memory
 speeds at petabyte scale.

<p align="center">
    <a href="https://www.gridgain.com/">
        <img src="https://files.readme.io/58b7901-gg_platform.png" width="600px"/>
    </a>
</p>

## Memory-Centric Storage
GridGain is based on distributed memory-centric architecture that combines the performance and scale of in-memory
computing together with the disk durability and strong consistency in one system.

The main difference between the memory-centric approach and the traditional disk-centric approach is that the memory
is treated as a fully functional storage, not just as a caching layer, like most databases do.
For example, GridGain can function in a pure in-memory mode, in which case it can be treated as an
In-Memory Database (IMDB) and In-Memory Data Grid (IMDG) in one.

On the other hand, when persistence is turned on, GidGain begins to function as a memory-centric system where most of
the processing happens in memory, but the data and indexes get persisted to disk. The main difference here
from the traditional disk-centric RDBMS or NoSQL system is that GridGain is strongly consistent, horizontally
[Read More](https://apacheignite-net.readme.io/docs/durable-memory)

## Native Persistence

Native Persistence is a distributed, ACID, and SQL-compliant **disk store** that transparently integrates with
GridGain memory-centric storage as an optional disk layer storing data and indexes on SSD,
 Flash, 3D XPoint, and other types of non-volatile storages.

With the Native Persistence enabled, you no longer need to keep all the data and indexes in memory or warm it
up after a node or cluster restart because the Durable Memory is tightly coupled with persistence and treats
it as a secondary memory tier. This implies that if a subset of data or an index is missing in RAM,
the Durable Memory will take it from the disk.
<p align="center">
    <a href="https://apacheignite.readme.io/docs/distributed-persistent-store">
        <img src="https://ignite.apache.org/images/native_persistence.png?renew" width="400px"/>
    </a>
</p>

[Read More](https://apacheignite-net.readme.io/docs/ignite-persistent-store)

## ACID Compliance
Data stored in GridGain is ACID-compliant both in memory and on disk, making GridGain a **strongly consistent** system. GridGain transactions work across the network and can span multiple servers.

[Read More](https://apacheignite-net.readme.io/docs/transactions)

## Complete SQL Support
GridGain provides full support for SQL, DDL and DML, allowing users to interact with GridGain using pure SQL without writing any code. This means that users can create tables and indexes as well as insert, update, and query data using only SQL. Having such complete SQL support makes GridGain a one-of-a-kind **distributed SQL database**.

[Read More](https://apacheignite-net.readme.io/docs/sql-database)

## Key-Value
The in-memory data grid component in GridGain is a fully transactional **distributed key-value store** that can scale horizontally across 100s of servers in the cluster. When persistence is enabled, GridGain can also store more data than fits in memory and survive full cluster restarts.

[Read More](https://apacheignite-net.readme.io/docs/data-grid)

## Collocated Processing
Most traditional databases work in a client-server fashion, meaning that data must be brought to the client side for processing. This approach requires lots of data movement from servers to clients and generally does not scale. GridGain, on the other hand, allows for sending light-weight computations to the data, i.e. **collocating** computations with data. As a result, GridGain scales better and minimizes data movement.

[Read More](https://apacheignite-net.readme.io/docs/colocate-compute-and-data)

## Scalability and Durability
GridGain is an elastic, horizontally scalable distributed system that supports adding and removing cluster nodes on demand. GridGain also allows for storing multiple copies of the data, making it resilient to partial cluster failures. If the persistence is enabled, then data stored in GridGain will also survive full cluster failures. Cluster restarts in GridGain can be very fast, as the data becomes operational instantaneously directly from disk. As a result, the data does not need to be preloaded in-memory to begin processing, and GridGain caches will lazily warm up resuming the in memory performance.

[Read More](https://apacheignite-net.readme.io/docs/cluster)

## Ignite and Ignite C++
* Ignite C++ is built on top of Ignite.
* Ignite C++ starts the JVM in the same process and communicates with it via JNI.
* .NET, C++ and Java nodes can join the same cluster, use the same caches, and interoperate using common binary protocol.
* Java compute jobs can execute on any node (Java, .NET, C++).
* C++ compute jobs can only execute on C++ nodes.

There is also C++ Thin Client (see `IgniteClient`), which does not start JVM and does not require Java on machine.

## GridGain Components

You can view GridGain as a collection of independent, well-integrated components geared to improve performance and
 scalability of your application.

Some of these components include:
* [Data Grid](##data-grid-jcache)
* [SQL Database](#sql-database)
* [Compute Grid](#compute-grid)

Below you’ll find a brief explanation for each of them:

## Data Grid (Cache)

[GridGain data grid](https://www.gridgain.com/docs/latest/developers-guide/key-value-api/basic-cache-operations) is an in-memory distributed key-value store which can be viewed as a distributed partitioned `map`, with every cluster node
owning a portion of the overall data. This way the more cluster nodes we add, the more data we can cache.

Unlike other key-value stores, GridGain determines data locality using a pluggable hashing algorithm. Every client can determine which node a key
belongs to by plugging it into a hashing function, without a need for any special mapping servers or name nodes.

GridGain data grid supports local, replicated, and partitioned data sets and allows to freely cross query between these data sets using standard SQL and LINQ syntax.
GridGain supports standard SQL and LINQ for querying in-memory data including support for distributed joins.

<p align="center">
    <a href="https://www.gridgain.com/docs/latest/developers-guide/key-value-api/basic-cache-operations">
        <img src="https://ignite.apache.org/images/data_grid.png" vspace="15" width="450px"/>
    </a>
</p>

## SQL Database

GridGain incorporates [distributed SQL database](https://www.gridgain.com/docs/latest/developers-guide/SQL/sql-introduction) capabilities as a part of its platform. The database is horizontally
 scalable, fault tolerant and SQL ANSI-99 compliant. It supports all SQL, DDL, and DML commands including SELECT, UPDATE,
  INSERT, MERGE, and DELETE queries. It also provides support for a subset of DDL commands relevant for distributed
  databases.

With GridGain Durable Memory architecture, data as well as indexes can be stored both in memory and, optionally, on disk.
This allows executing distributed SQL operations across different memory layers achieving in-memory performance with the durability of disk.

You can interact with GridGain using the SQL language via natively developed APIs for Java, .NET and C++, or via
the GridGain JDBC or ODBC drivers. This provides a true cross-platform connectivity from languages such as PHP, Ruby and more.


<p align="center">
    <a href="https://www.gridgain.com/docs/latest/developers-guide/SQL/sql-introduction">
        <img src="https://ignite.apache.org/images/sql_database.png" vspace="15" width="400px"/>
    </a>
</p>

## Compute Grid

[Distributed computations](https://www.gridgain.com/docs/latest/developers-guide/distributed-computing/distributed-computing) are performed in parallel fashion to gain high performance, low latency, and linear scalability.
GridGain compute grid provides a set of simple APIs that allow users distribute computations and data processing across multiple computers in the cluster.
Distributed parallel processing is based on the ability to take any computation and execute it on any set of cluster nodes and return the results back.

<p align="center">
    <a href="https://www.gridgain.com/docs/latest/developers-guide/distributed-computing/distributed-computing">
        <img src="https://ignite.apache.org/images/collocated_processing.png" vspace="15" width="400px"/>
    </a>
</p>


[apache-ignite-homepage]: https://ignite.apache.org/
[GridGain-homepage]: https://www.gridgain.com/
[getting-started]: https://docs.gridgain.com/docs
[docs]: https://docs.gridgain.com/docs
