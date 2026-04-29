# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GridGain Community Edition 8.9 — a distributed in-memory computing platform built on Apache Ignite. The codebase is a Java Maven multi-module project (58+ modules) targeting Java 1.8 bytecode, built and tested with Java 11+.

## Build Commands

```bash
# Full build, skip tests (most common)
mvn clean install -Pall-java,all-scala,licenses -DskipTests

# Build a single module and its dependencies
mvn clean install -pl :ignite-core -am -DskipTests

# Validate code style (Checkstyle)
mvn validate -Pcheckstyle

# Validate license headers (Apache RAT)
mvn clean validate -Pcheck-licenses
```

## Running Tests

```bash
# Run a specific test class (from repo root)
mvn clean test -U -Pexamples,-clean-libs,-release \
  -Dmaven.test.failure.ignore=true \
  -DfailIfNoTests=false \
  -Dtest=GridCacheLocalAtomicFullApiSelfTest

# Run a specific test method
mvn clean test -U -Pexamples,-clean-libs,-release \
  -Dmaven.test.failure.ignore=true \
  -DfailIfNoTests=false \
  -Dtest=GridCacheLocalAtomicFullApiSelfTest#testGet

# Run a test suite
mvn clean test -U -Pexamples,-clean-libs,-release \
  -Dmaven.test.failure.ignore=true \
  -DfailIfNoTests=false \
  -Dtest=IgniteBasicTestSuite

# Run tests in a specific module
mvn test -pl :ignite-core -am
```

Test suites live in `modules/core/src/test/java/org/apache/ignite/testsuites/` (116+ suites). Flaky/skipped tests are tracked in `modules/ignored-tests/`.

## Module Structure

Key modules:

| Module | Artifact ID | Purpose |
|--------|-------------|---------|
| `modules/core` | `ignite-core` | Main engine: cache, cluster, compute, transactions |
| `modules/indexing` | `ignite-indexing` | SQL indexing via H2 |
| `modules/spring` | `ignite-spring` | Spring integration |
| `modules/compress` | `ignite-compress` | Data compression |
| `modules/direct-io` | `ignite-direct-io` | Direct I/O for persistence |
| `modules/ml` | `ignite-ml` | Machine learning |
| `modules/zookeeper` | `ignite-zookeeper` | ZooKeeper discovery SPI |
| `modules/kubernetes` | `ignite-kubernetes` | Kubernetes IP finder |
| `modules/control-utility` | `ignite-control-utility` | CLI management tool |
| `modules/compatibility` | `ignite-compatibility` | Cross-version compatibility tests |

Dependency versions are centrally managed in `parent/pom.xml`.

## Architecture

### Public API (`modules/core/src/main/java/org/apache/ignite/`)

The `Ignite` interface is the main entry point. Key sub-APIs obtained from it:
- `IgniteCache` — distributed cache with SQL/predicate queries
- `IgniteCluster` / `ClusterGroup` — topology and node management
- `IgniteCompute` — distributed task/closure execution
- `IgniteTransactions` — distributed ACID transactions
- `IgniteServices` — deployed singleton services
- `IgniteDataStreamer` — bulk data ingestion
- `IgniteMessaging` / `IgniteEvents` — pub/sub and event system
- Data structures: `IgniteAtomicLong`, `IgniteQueue`, `IgniteSet`, etc.

### Internal Implementation (`modules/core/src/main/java/org/apache/ignite/internal/`)

The internal package is **not public API**. Architecture is processor-based:
- `GridProcessor` subclasses handle each major subsystem (cache, compute, events, transactions, etc.)
- SPI (Service Provider Interfaces) under `org.apache.ignite.spi` allow pluggable implementations for discovery, load balancing, communication, checkpoint, failover, and collision
- `RendezvousAffinityFunction` handles partition-to-node mapping

### Key Design Patterns
- SPI pattern: most behaviors are swappable via `IgniteConfiguration` (e.g., `TcpDiscoverySpi`, `TcpCommunicationSpi`)
- Processors are wired together at grid startup via `GridKernalContext`
- Test infrastructure uses `GridAbstractTest` base class with embedded cluster startup

## Code Style

- Checkstyle rules: `checkstyle/checkstyle.xml` (suppressions in `checkstyle-suppressions.xml`)
- No tabs — spaces only
- UTF-8 encoding, Unix line endings
- `@Override` annotations required
- Unused imports prohibited
- Modifier order enforced

## Branch & Commit Conventions

- Feature branches follow `GG-XXXXX` pattern (JIRA ticket number)
- Main development branch: `master`
- Release branches: `8.9-master`, `8.8-master`

## SQL Function Registration Race: Create Cache Before Starting Extra Nodes

### Symptom

```
Exception message is not as expected [expected=cancel,
  actual=Failed to parse query. Function "LONGPROCESS" not found; SQL statement:
  select longProcess(_val, 8) from default [90022-199]]
```

The test expected a query to be cancelled by timeout, but H2 couldn't even parse it because a custom SQL function (`@QuerySqlFunction`) was not registered on the node that executed the query.

### Root cause

`setSqlFunctionClasses(SomeClass.class)` on a `CacheConfiguration` registers custom H2 SQL functions when a node **initializes that cache locally**. When a cache is created dynamically (`ign.createCache(cfg)`) _after_ additional nodes have already joined the cluster, those nodes receive a cache-start event and initialize the cache asynchronously. If a SQL query is dispatched from such a node before its local cache initialization completes, H2 cannot resolve the function and the query fails at parse time with "Function … not found".

The pattern that triggers this:

```java
startGrid(0);
prepareQueryExecution();    // starts client grid — joins BEFORE cache exists
helper.createCache(grid(0)); // cache created after client joined
executeQuery(...);           // client parses query — function may not yet be registered
```

### Rule: create shared caches before starting additional nodes

Any test that uses `setSqlFunctionClasses` (or any `@QuerySqlFunction`) must create the cache **before** starting client grids or other nodes that will execute queries against it. Nodes that join _after_ the cache already exists initialize it synchronously during the join handshake, so the function is guaranteed to be available before any query is dispatched.

```java
startGrid(0);
helper.createCache(grid(0)); // cache created first
prepareQueryExecution();     // client starts and sees cache already present — initializes synchronously
executeQuery(...);           // safe: function registered
```

### Known fix

`AbstractDefaultQueryTimeoutTest` — swapped `prepareQueryExecution()` (which starts a client grid) to run **after** `helper.createCache(grid(0))` in both `checkQuery0` and `testConcurrent`. Applied to the same file because the pattern was repeated in both methods.

## Test Isolation: LOCAL_IP_FINDER vs sharedStaticIpFinder

`GridAbstractTest` provides two IP finders for discovery in tests:

- **`sharedStaticIpFinder`** — in-memory, per-JVM, initialized empty each test class. Nodes register their actual ports dynamically. Safe for parallel CI forks because it never crosses JVM boundaries. This is the default used by the framework (`getConfiguration` sets it when `isMultiJvm()` is false).
- **`LOCAL_IP_FINDER`** — static, fixed addresses `127.0.0.1:47500..47509`. Used for multi-JVM tests so remote child JVMs can find the cluster. Because it tries to TCP-connect to those fixed ports, it **crosses JVM boundaries** at the OS level and can discover nodes from unrelated parallel CI forks.

### Rule: never use LOCAL_IP_FINDER in single-JVM tests

Any single-JVM test that sets `discoSpi.setIpFinder(LOCAL_IP_FINDER)` instead of `sharedStaticIpFinder` is a flakiness source: when a parallel CI fork has nodes bound to ports 47500–47509 with different configuration attributes (e.g. `peerClassLoading=false`), the test discovers those foreign nodes and `GridDiscoveryManager.checkAttributes` throws, failing startup.

Symptom: `"Remote node has peer class loading enabled flag different from local"` with a remote node `order` much higher than expected (indicating it belongs to another test's cluster).

### Rule: tests with peerClassLoading=false must avoid ports 47500–47509

Tests that explicitly set `peerClassLoading=false` should bind their discovery SPI to a port range **outside** 47500–47509 (e.g. 47610–47619) so that `LOCAL_IP_FINDER`-based tests in parallel forks cannot discover them.

## HTTP Server Tests: Use OS-Assigned Ports, Not Hardcoded Ones

### Symptom

```
java.io.IOException: Failed to bind to 0.0.0.0/0.0.0.0:49090
Caused by: java.net.BindException: Address already in use
```

### Root cause

Any test that starts an embedded HTTP server (Jetty, Netty, etc.) on a hardcoded port will fail with `BindException` when a parallel CI fork runs the same test concurrently and binds to the same port first.

### Rule: always use port 0 (OS-assigned) for embedded HTTP servers in tests

Pass `0` as the port to `new Server(0)` (Jetty) or equivalent. After `srv.start()`, read the actual port back:

```java
int port = ((ServerConnector)srv.getConnectors()[0]).getLocalPort();
```

Store the result in a non-static instance field so all subsequent URL constructions in the same test method use the correct port.

### Known fixes applied

| Test | Problem | Fix |
|------|---------|-----|
| `WebSessionSelfTest` (+ subclasses) | `TEST_JETTY_PORT = 49090` hardcoded; parallel forks bind to the same port | Changed `static final int TEST_JETTY_PORT` to `int TEST_JETTY_PORT` (instance field); both `startServer` / `startServerWithLoginService` now use `new Server(0)` and write `TEST_JETTY_PORT` from the actual `ServerConnector.getLocalPort()` after start |

### Known fixes applied

| Test | Problem | Fix |
|------|---------|-----|
| `TcpDiscoveryNetworkIssuesTest` | Used ports 47500–47505 (hardcoded `NODE_X_PORT` constants) with its own `TcpDiscoveryVmIpFinder`. A parallel fork using `LOCAL_IP_FINDER` connects to those ports and joins the ring; the joining node's UUID is recorded in `nodesIdsHist`. The test's own node 1 (next test run) tries to join with a UUID derived from instance name "node01-47501" (deterministic low/high nibbles) and is rejected by the `nodesIdsHist` check. | Changed `NODE_0_PORT`–`NODE_5_PORT` from 47500–47505 to 47610–47615. |

### Known fixes applied (IgniteSpiCommunicationSelfTestSuite)

| Test | Problem | Fix |
|------|---------|-----|
| `TcpCommunicationSpiSkipMessageSendTest` | Used `LOCAL_IP_FINDER` in a single-JVM test | Changed to `sharedStaticIpFinder` |
| `IgniteTcpCommunicationHandshakeWaitTest` (+ SSL variant) | Sets `peerClassLoading=false`; nodes bind to default port 47500 | Added `setLocalPort(47610)` / `setLocalPortRange(10)` on the discovery SPI |

## `stopGrid` in Background Threads: Never Await Topology

### Symptom

```
java.lang.RuntimeException: Not all Ignite instances has been stopped. Please, see log for details.
```

Preceded by `[WARN] Interrupting threads started so far: N` and multiple `[ERROR] Failed to stop grid ... IgniteInterruptedCheckedException: sleep interrupted` from background threads calling `stopGrid`.

### Root cause

`stopGrid(name)` (and the two-arg form `stopGrid(name, cancel)`) defaults to `awaitTop=true`, which calls `awaitTopologyChange()`. `awaitTopologyChange` polls via `waitForCondition → IgniteUtils.sleep`. When multiple background threads concurrently stop nodes, each thread's `awaitTopologyChange` waits for the topology to stabilize, but the other threads keep triggering new topology events — so they all block each other indefinitely.

The test framework eventually fires its global timeout and calls `GridTestUtils.stopThreads(log)`, which interrupts all background threads. The interrupted `IgniteUtils.sleep` throws `IgniteInterruptedCheckedException`, which is caught in `stopGrid0`, sets `stopGridErr = true`, and leaves the node running. After the test, `stopAllGrids` finds `stopGridErr=true` and throws the `RuntimeException`.

### Rule: background restart threads must use `stopGrid(name, false, false)`

Any test thread that repeatedly starts and stops nodes for topology churn (not for topology-stability verification) must pass `awaitTop=false`:

```java
stopGrid(name, false, false);   // cancel=false, awaitTop=false
```

`awaitTop=true` is only appropriate in the main test thread after a deliberate topology change where the test logic needs to observe a stable state.

### Known fixes applied

| Test | Fix |
|------|-----|
| `MemLeakOnSqlWithClientReconnectTest.checkReservationLeak` — 10 concurrent cli-restart threads each called `stopGrid(name)` (awaitTop=true), causing mutual blocking → interrupt → `stopGridErr=true` | Changed to `stopGrid(name, false, false)` |

## One-Phase Commit Wrapper Leaked After Near-Node Rollback (`IgniteCachePutRetryTransactionalSelfTest`)

### Symptom

```
java.lang.AssertionError: completedVersHashMap contains
  org.apache.ignite.internal.processors.cache.GridCacheReturnCompletableWrapper
  instead of boolean. These values should be replaced by boolean after
  onePhaseCommit finished. [node=dht.IgniteCachePutRetryTransactionalSelfTest3]
```

Fired by `checkOnePhaseCommitReturnValuesCleaned` in `afterTest` even after the 5-second wait.

### Root cause

`GridCacheReturnCompletableWrapper` is put into `IgniteTxManager.completedVersHashMap` on the **backup** node (node3) when a one-phase-commit implicit transaction with `needReturnValue=true` (e.g. `cache.invoke`) is processed. It should be replaced by `true` (a boolean) once the near node sends a deferred ack (`GridDhtTxOnePhaseCommitAckRequest`).

The ack is sent by `ackBackup()`, called in the `finally` block of `GridNearTxFinishFuture.doFinish`:

```java
finally {
    if (commit &&              // ← old: guarded by commit
        tx.onePhaseCommit() &&
        !tx.writeMap().isEmpty())
        ackBackup();
}
```

When a topology change causes the near node to **retry** the transaction (calling `doFinish(false, ...)`), `commit=false` so `ackBackup()` is skipped. The backup already committed one-phase and holds the wrapper. The ack is never sent, and the wrapper stays indefinitely because:

- The `EVT_NODE_LEFT` cleanup path in `IgniteTxManager` only fires when the **originating** node leaves — in this test node3 (the backup) is restarted, not node0 (the originator).
- The deferred-ack path in `ackBackup()` is bypassed by the `commit &&` guard.

### Fix

`GridNearTxFinishFuture.doFinish` — drop `commit &&` from the guard:

```java
finally {
    if (tx.onePhaseCommit() &&
        !tx.writeMap().isEmpty())
        ackBackup();
}
```

`ackBackup()` itself already guards on `tx.needReturnValue() && tx.implicit()` (the only cases where a wrapper is stored on the backup). When called on rollback it sends the deferred ack unconditionally; on the backup `removeTxReturn` replaces the wrapper with `true` if one exists, or is a no-op if the backup never committed.

### Note

`testOriginatingNodeFailureForcesOnePhaseCommitDataCleanup` in the same class covers the complementary case (originating node leaves) and already passes because `EVT_NODE_LEFT` triggers cleanup. The bug only manifests when the **backup** is restarted while the originator stays alive.

## `JdbcThinConnection.cliIo` Index Out of Bounds on Node Round-Robin

### Symptom

```
java.sql.SQLException: Failed to communicate with Ignite cluster.
Caused by: java.lang.IndexOutOfBoundsException: Index: 2, Size: 2
    at JdbcThinConnection.cliIo(JdbcThinConnection.java:1698)
```

Seen in `JdbcThinPartitionAwarenessSelfTest` when partition-awareness routes a request to one of multiple nodes and the chosen I/O object is absent from `ios`.

### Root cause

`JdbcThinConnection.cliIo` (the partition-awareness node-selection branch) cycles through `nodeIds` to find a live `JdbcThinTcpIo`. The wrap-around guard was:

```java
initNodeId = initNodeId == nodeIds.size() ? 0 : initNodeId + 1;
```

The condition `initNodeId == nodeIds.size()` can never be true **before** `nodeIds.get(initNodeId)` on the previous line throws — by the time the check would wrap, the index is already one past the end. With `nodeIds.size() == 2` and `initNodeId` starting at 1:

- Iteration 1: `nodeIds.get(1)` → OK, suppose `ios` miss → `io` stays `null`; `initNodeId` becomes 2 (guard doesn't fire because `1 != 2`)
- Iteration 2: `nodeIds.get(2)` → `IndexOutOfBoundsException`

### Fix

`JdbcThinConnection.java` line 1700 — replaced the broken guard with modulo arithmetic:

```java
// before
initNodeId = initNodeId == nodeIds.size() ? 0 : initNodeId + 1;
// after
initNodeId = (initNodeId + 1) % nodeIds.size();
```

## Partition Reservation Leak on Unstable Client Topology (`MemLeakOnSqlWithClientReconnectTest`)

### Symptom

```
java.lang.AssertionError: Reservations leaks: [base=1, cur=2]
```

`PartitionReservationManager.reservations` map grows from 1 to 2 entries during `testPartitioned` while 10 client nodes repeatedly join and leave the cluster.

### Root cause

`PartitionReservationManager.reservePartitions` computes the group-reservation cache key as `(cacheName, lastAffinityChangedTopologyVersion(reqTopVer))`. When a client joins (overall topology T3, affinity unchanged at T1), a query can arrive with `reqTopVer=T3` before the server has finished T3's partition-map exchange. At that moment `lastAffinityChangedTopologyVersion(T3)` returns T3 itself (no entry yet in `lastAffTopVers`), so a **stale** group reservation keyed `(part, T3)` is created alongside the legitimate `(part, T1)` entry.

After T3's exchange completes, `onDoneAfterTopologyUnlock` tries `invalidate()` on `(part, T3)`. `invalidate()` requires `reservations.count == 0`; if the query is still running, it fails and the stale entry stays. When the query later finishes and `release()` drops the count to 0, **no one retried the invalidation**, leaving `(part, T3)` permanently in the map.

### Fix

`GridDhtPartitionsReservation.release()` — after the last reservation is released, check whether `lastAffinityChangedTopologyVersion(this.topVer) != this.topVer`. If so, the reservation was keyed to a non-affinity topology version (i.e. it is stale) and `invalidate()` is called immediately.

- A valid entry (`topVer = T1`, a true affinity-change version): `lastAffinityChangedTopologyVersion(T1) = T1` → check is false → not invalidated.
- A stale entry (`topVer = T3`, client-only change): `lastAffinityChangedTopologyVersion(T3) = T1 ≠ T3` → check is true → `invalidate()` → removed from map.

If T3's exchange hasn't finished by the time `release()` runs, the check is false (still returns T3) and the existing `onDoneAfterTopologyUnlock` path handles cleanup once the exchange completes.

## Atomic Future Stuck in INIT: `hasRes` Race in `PrimaryRequestState.onDhtResponse`

### Symptom

```
java.lang.AssertionError: Unexpected atomic futures: [GridNearAtomicSingleUpdateFuture
  [reqState=Primary [..., primaryRes=false, done=true, ...],
   super=GridNearAtomicAbstractUpdateFuture [..., state=INIT, ...]]]
```

The future is in the `atomicFutures` map, its `GridFutureAdapter.state` is `INIT` (never completed), yet `reqState.done=true` (`finished()` returns `true`). Seen in `IgniteCacheSslStartStopSelfTest` (extends `IgniteCachePutRetryAbstractSelfTest`), which runs FULL_SYNC ATOMIC puts while repeatedly stopping/starting a backup node.

### Root cause

In `PrimaryRequestState.onDhtResponse` (`GridNearAtomicAbstractUpdateFuture.java`), `hasRes = true` was set **before** the duplicate-node check. The following race triggered it:

1. Near node maps a put (FULL_SYNC, `initMappingLocally=true`, 1 backup). Request sent to primary; primary forwards to backup.
2. Backup **leaves** topology → `onNodeLeft` fires → `onDhtNodeLeft` increments `rcvdCnt` to `expCnt`, but `hasRes` is still `false` → returns `ALL_RCVD_CHECK_PRIMARY` → a `GridNearAtomicCheckUpdateRequest` is sent to the primary.
3. The backup's **in-flight DHT response** (sent before it left) arrives at the near node. `onDhtResponse` is called:
   - Old code: `hasRes = true` set immediately (because `res.hasResult()`), **then** `nodeRes.rcvd == true` detected → `return false`.
   - Result: `hasRes=true`, `rcvdCnt=expCnt=1` → `finished()=true`, but the future was never completed.
4. The check response from the primary arrives → `processPrimaryResponse` → `finished()=true` → returns `null` → future abandoned permanently in INIT state.

SSL amplifies the window: handshake latency makes it likely the node-left event is processed **before** the in-flight response arrives, whereas in non-SSL tests the response wins the race.

### Fix

`GridNearAtomicAbstractUpdateFuture.java`, `PrimaryRequestState.onDhtResponse`: moved `hasRes = true` into each non-duplicate branch so a late response for an already-counted node cannot corrupt `finished()`:

- `mappedNodes == null` branch (unknown mapping): set `hasRes` here before returning.
- `nodeRes != null && !nodeRes.rcvd` branch (new response): set `hasRes` here.
- `nodeRes == null` branch (unexpected node): set `hasRes` here.
- `nodeRes != null && nodeRes.rcvd` branch (duplicate): **return early without touching `hasRes`** — this is the fix.

### Note on `topVer` in the assertion output

The large `topVer` (e.g. 330) in exchange log lines near the assertion is just accumulated topology version from many stop/start cycles across ~11 test methods; it is unrelated to the stuck future's own `topVer` field.

## `startGrid` Does Not Guarantee Topology Propagation to Existing Nodes

### Symptom

```
org.apache.ignite.IgniteException: Failed to find cache (cache was not started yet or cache was already stopped): partitioned
    at GridAffinityProcessor$CacheAffinityProxy.cache(GridAffinityProcessor.java:1132)
    at GridAffinityProcessor$CacheAffinityProxy.mapKeyToNode(...)
```

Thrown immediately after all worker nodes are started, before any cache operations have begun.

### Root cause

`startGrid("workerX")` waits only for the new node's own partition-map exchange to complete. It does **not** wait for already-running nodes (e.g. a master client node) to finish processing the topology-change event that includes the new node's caches. When a test starts a master client first and then starts worker nodes that define the cache, the master may still be processing the join exchange asynchronously when the test proceeds to call `affinity(cacheName)` or `cache(cacheName)`.

```java
final Ignite master = startGrid("master");   // client, no cache configured

for (int i = 1; i <= workerCnt; i++)
    workers.add(startGrid("worker" + i));    // workers bring "partitioned" cache

// RACE: master may not have processed worker join exchanges yet
master.affinity("partitioned").mapKeyToNode("Dummy");  // ← fails
```

### Rule: call `awaitPartitionMapExchange()` before using a cache that was defined by later-joining nodes

After starting all nodes that carry the cache definition, call `awaitPartitionMapExchange()` to guarantee that every node in the cluster — including the master client — has completed the exchange and has the cache initialized:

```java
for (int i = 1; i <= workerCnt; i++)
    workers.add(startGrid("worker" + i));

awaitPartitionMapExchange();  // all nodes, including master, have processed all joins

master.affinity("partitioned").mapKeyToNode("Dummy");  // safe
```

### Known fix

`GridCachePutAllFailoverSelfTest.checkPutAllFailover` — added `awaitPartitionMapExchange()` after the workers-start loop and before the `try` block that calls `master.affinity(CACHE_NAME).mapKeyToNode("Dummy")`.

## Dynamic Cache Leak: `createCache` Outside `try-finally` Leaves Cache on Exception

### Symptom

```
org.apache.ignite.cache.CacheExistsException: Failed to start cache
    (a cache with the same name is already started): test-cache-PRIMARY_SYNC-2
```

Thrown at the very first `createCache` call of a test method, before any test logic runs.

### Root cause

A common pattern in tests is:

```java
grid(0).createCache(ccfg);         // ← OUTSIDE the try block

IgniteCache clnCache = cln.createNearCache(...);  // ← can throw
clnCache.put(KEY, VALUE);                         // ← can throw

try {
    // test logic
}
catch (Exception e) { e.printStackTrace(); }
finally {
    grid(0).destroyCache(ccfg.getName());  // ← only reached if no exception above
}
```

If anything between `createCache` and the `try` block throws (e.g. `createNearCache` when a near-cache config is present, or even `cache.put`), the `finally` is never reached and the cache is left alive on the cluster. The next test method (or the next iteration) that tries to create a cache with the same name then fails with `CacheExistsException`.

Multiple test methods in the same class can share the same cache name if they use identical `CacheWriteSynchronizationMode` and backup-count combinations — e.g. both `testOptimistic` and `testOptimisticWithNearCache` produce `"test-cache-PRIMARY_SYNC-2"`.

### Rule: always place `createCache` inside the `try` block that has the `finally { destroyCache }` guard

```java
try {
    grid(0).createCache(ccfg);

    IgniteCache clnCache = cln.createNearCache(...);
    clnCache.put(KEY, VALUE);

    // test logic
}
catch (Exception e) { e.printStackTrace(); }
finally {
    grid(0).destroyCache(ccfg.getName());  // always runs
}
```

`destroyCache` on a cache name that doesn't exist is a no-op, so it is safe to call it even when `createCache` was the call that failed.

### Known fix

`CacheEntryProcessorExternalizableFailedTest.doTestInvokeTest` — moved `grid(0).createCache(ccfg)`, `cln.createNearCache(...)`, and `clnCache.put(KEY, EXPECTED_VALUE)` inside the single `try` block so that `destroyCache` in `finally` always executes.

## Schema Operation Race With Concurrent Cache Destroy (`DynamicColumns*ConcurrentCacheDestroy`)

### Symptom

```
SchemaOperationException [code=1, msg=Cache doesn't exist: SQL_PUBLIC_PERSON]
  at GridQueryProcessor.startSchemaChange(GridQueryProcessor.java:880)
  at GridQueryProcessor.onSchemaPropose(GridQueryProcessor.java:736)
  at GridCacheProcessor.processCustomExchangeTask(GridCacheProcessor.java:423)
  at GridCachePartitionExchangeManager$ExchangeWorker.processCustomTask(...)
```

Thrown from `idxFut.get()` in `DynamicColumnsAbstractConcurrentSelfTest.checkConcurrentCacheDestroy`, causing `testDropConcurrentCacheDestroy` (and potentially `testAddConcurrentCacheDestroy`) to fail.

### Root cause

`DROP TABLE` in GridGain is implemented via `GridQueryProcessor.dynamicTableDrop` → `ctx.grid().destroyCache0(cacheName, true)`. This goes through the **partition exchange** (a `CacheDestroyRequest`), not the schema propose protocol. It is therefore not serialized with a concurrent `DROP COLUMN` schema change.

The schema propose protocol enqueues a `SchemaExchangeWorkerTask` in the exchange worker's `futQ`. If the cache-destroy partition exchange is processed before that task is dequeued (e.g., due to queue ordering or exchange delay), the exchange worker calls `onSchemaPropose` → `startSchemaChange` after the cache is already gone. At line 880, `cacheDesc == null` → `SchemaOperationException(CODE_CACHE_NOT_FOUND, cacheName)` is created and passed to the `SchemaOperationWorker`, which completes the schema operation future with that error.

### Rule: wrap `idxFut.get()` in concurrent-destroy tests to accept `CODE_CACHE_NOT_FOUND`

Tests that explicitly exercise concurrent schema-change + cache-destroy must not call `idxFut.get()` bare. The schema operation legitimately fails with `CODE_CACHE_NOT_FOUND` when the cache is destroyed before the operation is applied. Wrap the call and re-throw anything that is not the expected error code:

```java
try {
    idxFut.get();
}
catch (IgniteCheckedException e) {
    SchemaOperationException schemaEx = X.cause(e, SchemaOperationException.class);

    if (schemaEx == null || schemaEx.code() != SchemaOperationException.CODE_CACHE_NOT_FOUND)
        throw e;
}
dropFut.get();
```

Do **not** use `@Ignore("Flaky test")` for this class of failure — the test is structurally sound; only `idxFut.get()` needs to tolerate the expected concurrent error.

### Known fix

`DynamicColumnsAbstractConcurrentSelfTest.checkConcurrentCacheDestroy` — wrapped `idxFut.get()` with the try-catch above. Fixes both `testDropConcurrentCacheDestroy` and `testAddConcurrentCacheDestroy` across all subclasses (atomic replicated, transactional partitioned, etc.).

## Affinity History Exhausted in Long Test Classes (`DynamicIndexServerCoordinatorBasicSelfTest`)

### Symptom

```
java.lang.IllegalStateException: Getting affinity for too old topology version that is already
  out of history (try to increase 'IGNITE_MIN_AFFINITY_HISTORY_SIZE' or
  'IGNITE_AFFINITY_HISTORY_SIZE' system properties)
  [locNode=..., grp=cache,
   topVer=AffinityTopologyVersion [topVer=5, minorTopVer=124],
   lastAffChangeTopVer=AffinityTopologyVersion [topVer=5, minorTopVer=124],
   head=AffinityTopologyVersion [topVer=5, minorTopVer=125],
   history=[AffinityTopologyVersion [topVer=5, minorTopVer=125]],
   minNonShallowHistorySize=2, maxNonShallowHistorySize=25]
  at GridAffinityAssignmentCache.cachedAffinity(GridAffinityAssignmentCache.java:848)
  at GridCommonAbstractTest.awaitPartitionMapExchange(GridCommonAbstractTest.java:873)
  at DynamicIndexAbstractBasicSelfTest.initialize(DynamicIndexAbstractBasicSelfTest.java:100)
```

### Root cause

`GridAffinityAssignmentCache` tracks affinity assignments in a bounded history map. Each real topology-affinity change (e.g. cache create) is a **non-shallow** entry. When the count of non-shallow entries exceeds `MAX_NON_SHALLOW_HIST_SIZE` (default 25), the cleanup logic fires and **aggressively removes** old entries until only `MIN_NON_SHALLOW_HIST_SIZE` (default 2) remain.

`DynamicIndexAbstractBasicSelfTest` runs ~30+ sequential test methods, each calling `initialize()` which creates a new SQL cache — generating one non-shallow affinity entry per method. After ~26 methods the cleanup triggers and reduces the history to 2 entries. Meanwhile `awaitPartitionMapExchange` reads `readyAffinityVersion()` returning `minorTopVer=124` for a node, then immediately calls `assignment(124)` — but the cleanup already evicted `124`, leaving only `125`. The `ceilingEntry(124)` call finds `125`, which is **greater** than the requested version, so `cachedAffinity` throws.

The cleanup loop iterates ascending (oldest-first) and stops at the first non-shallow entry where `nonShallowSize <= MIN_NON_SHALLOW_HIST_SIZE`. All entries before that stop point — including the version `awaitPartitionMapExchange` just observed — are removed.

### Fix: raise `IGNITE_AFFINITY_HISTORY_SIZE` globally in `GridAbstractTest`

`MAX_NON_SHALLOW_HIST_SIZE` and `MIN_NON_SHALLOW_HIST_SIZE` are **instance fields** in `GridAffinityAssignmentCache`, re-read from system properties at each object construction. Setting the property before nodes start is sufficient.

Added to `GridAbstractTest`'s static initializer (alongside other test-wide property overrides):

```java
System.setProperty(IGNITE_AFFINITY_HISTORY_SIZE, "500");
```

With a limit of 500, cleanup only fires after 500 non-shallow entries — far beyond what any realistic test run accumulates — so the history is never trimmed and `awaitPartitionMapExchange` always finds the version it needs.

### Known fix

`GridAbstractTest` static initializer — added `System.setProperty(IGNITE_AFFINITY_HISTORY_SIZE, "500")`. Applies to every test that extends `GridAbstractTest` (the entire test hierarchy).

## Thin Client Cache Lookup Fails Inside Resumed Transaction (`BlockingTxOpsTest.testTransactionalConsistency`)

### Symptom

```
org.apache.ignite.client.ClientException: Ignite failed to process request [5]:
  Cannot start/stop cache within lock or transaction
  [cacheNames=test, operation=dynamicStartCache] (server status code [1])
  at TcpClientCache.txAwareService(TcpClientCache.java:1277)
  at TcpClientCache.cacheSingleKeyOperation(TcpClientCache.java:1344)
  at TcpClientCache.get(TcpClientCache.java:173)
  at BlockingTxOpsTest.lambda$testTransactionalConsistency$54(BlockingTxOpsTest.java:318)
```

Thrown from `cache.get(key)` inside a thin client transaction.

### Root cause

The thin client server-side request handler (`ClientRequestHandler`) resumes the server-side transaction on the worker thread before calling `handle0(req)`, making the thread "inside a transaction" for the duration of the request.

`ClientCacheRequest.rawCache()` then calls `ctx.kernalContext().grid().cache(cacheName)`, which internally calls `GridCacheProcessor.publicJCache(cacheName, false, checkThreadTx=true)` — the `true` is hardcoded in `IgniteKernal.cache(String)`.

`publicJCache` calls `jcacheProxy(cacheName, true)` to fetch the running cache proxy. During the brief window while the cache's partition-map exchange is still completing on a server node (race between `getOrCreateCache` completing on the coordinator and propagating to all nodes), the proxy can be **absent from `jCacheProxies`**. When the proxy is null, `publicJCache` calls `dynamicStartCache(null, cacheName, null, false, false, checkThreadTx=true)`. Because `checkThreadTx=true` and the thread has a resumed transaction, `dynamicStartCache` throws "Cannot start/stop cache within lock or transaction" — even though the real intent of this `dynamicStartCache` call is just "wait for the locally pending exchange to complete", not "create a new cache."

The OPTIMISTIC+SERIALIZABLE variant is more susceptible because the exchange timing window is wider compared to PESSIMISTIC+REPEATABLE_READ.

### Fix

`ClientCacheRequest.rawCache()` — replaced `ctx.kernalContext().grid().cache(cacheName)` (which uses `checkThreadTx=true`) with a direct call to `ctx.kernalContext().cache().publicJCache(cacheName, false, false)` (`checkThreadTx=false`). Added a null-check so a truly missing cache throws `IgniteClientException(CACHE_DOES_NOT_EXIST)` instead of NPE.

`dynamicStartCache` with a null config only waits for the cache to start locally — it does not perform any new distributed cache creation — so bypassing the transaction guard here is correct.

### Known fix

`ClientCacheRequest.rawCache()` — changed lookup from `grid().cache(cacheName)` to `ctx.kernalContext().cache().publicJCache(cacheName, false, false)`. Also added `IgniteCheckedException` → `IgniteException` wrapping and an explicit null check.
