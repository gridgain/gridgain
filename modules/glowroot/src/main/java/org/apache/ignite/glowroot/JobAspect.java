package org.apache.ignite.glowroot;

/**
 * "instrumentation": [
 {
 "captureKind": "transaction",
 "transactionType": "Ignite",
 "transactionNameTemplate": "IgniteCommit",
 "traceEntryMessageTemplate": "{{this}}",
 "timerName": "IgniteCommit",
 "className": "org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl",
 "methodName": "commit",
 "methodParameterTypes": []
 }
 ]
 */
public class JobAspect {
}
