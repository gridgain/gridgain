/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


namespace Apache.Ignite.Core.Tests.Cache.Query.Continuous;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Event;
using Apache.Ignite.Core.Cache.Query.Continuous;
using Apache.Ignite.Core.Impl.Cache.Event;
using Apache.Ignite.Core.Impl.Cache.Query.Continuous;
using NUnit.Framework;

/// <summary>
/// Unit tests for the async continuous query building blocks that do not require a running node:
/// <see cref="ContinuousQueryAsyncHandle{TK,TV,TInitial}"/>, <see cref="ContinuousQueryAsync"/> and
/// <see cref="ContinuousQueryOptions"/>.
/// </summary>
public class ContinuousQueryAsyncTest
{
    /// <summary>
    /// Tests that <see cref="ContinuousQueryAsyncHandle{TK,TV,TInitial}.GetEvents"/> streams every event
    /// written to the channel and, when the stream ends, stops the query (mirrors the streaming overload).
    /// </summary>
    [Test]
    public async Task GetEvents_StreamsEventsThenStopsQueryOnCompletion()
    {
        var channel = Channel.CreateUnbounded<ICacheEntryEvent<int, int>>();
        var stopSpy = new DisposeSpy();
        var handle = new ContinuousQueryAsyncHandle<int, int, string>(stopSpy, channel, () => "cursor");

        channel.Writer.TryWrite(Event(1));
        channel.Writer.TryWrite(Event(2));
        channel.Writer.TryComplete();

        var keys = new List<int>();

        await foreach (var evt in handle.GetEvents())
        {
            keys.Add(evt.Key);
        }

        Assert.AreEqual(new[] { 1, 2 }, keys);
        Assert.AreEqual(1, stopSpy.DisposeCount, "Query should be stopped when enumeration ends.");
    }

    /// <summary>
    /// Tests that breaking out of the event enumeration stops the underlying query, just like the
    /// streaming overload.
    /// </summary>
    [Test]
    public async Task GetEvents_BreakStopsQuery()
    {
        var channel = Channel.CreateUnbounded<ICacheEntryEvent<int, int>>();
        var stopSpy = new DisposeSpy();
        var handle = new ContinuousQueryAsyncHandle<int, int, string>(stopSpy, channel, () => "cursor");

        channel.Writer.TryWrite(Event(1));

        await foreach (var evt in handle.GetEvents())
        {
            Assert.AreEqual(1, evt.Key);
            break;
        }

        Assert.AreEqual(1, stopSpy.DisposeCount);
        Assert.IsTrue(channel.Reader.Completion.IsCompleted);
    }

    /// <summary>
    /// Tests that cancelling the enumeration (via <c>WithCancellation</c>) throws and stops the query.
    /// </summary>
    [Test]
    public void GetEvents_CancelledToken_StopsQueryAndThrows()
    {
        var channel = Channel.CreateUnbounded<ICacheEntryEvent<int, int>>();
        var stopSpy = new DisposeSpy();
        var handle = new ContinuousQueryAsyncHandle<int, int, string>(stopSpy, channel, () => "cursor");

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () =>
        {
            await foreach (var unused in handle.GetEvents().WithCancellation(cts.Token))
            {
                Assert.Fail("No events should be produced for a cancelled token.");
            }
        });

        Assert.AreEqual(1, stopSpy.DisposeCount);
    }

    /// <summary>
    /// Tests that <see cref="ContinuousQueryAsyncHandle{TK,TV,TInitial}.GetEvents"/> can be called only once.
    /// </summary>
    [Test]
    public void GetEvents_SecondCall_Throws()
    {
        var channel = Channel.CreateUnbounded<ICacheEntryEvent<int, int>>();
        var handle = new ContinuousQueryAsyncHandle<int, int, string>(new DisposeSpy(), channel, () => "cursor");

        handle.GetEvents();

        Assert.Throws<InvalidOperationException>(() => handle.GetEvents());
    }

    /// <summary>
    /// Tests that <see cref="ContinuousQueryAsyncHandle{TK,TV,TInitial}.GetInitialQueryCursor"/> delegates to
    /// the provided cursor factory (the "only once" semantics are enforced by the underlying handle).
    /// </summary>
    [Test]
    public void GetInitialQueryCursor_DelegatesToFactory()
    {
        var channel = Channel.CreateUnbounded<ICacheEntryEvent<int, int>>();
        var calls = 0;
        var handle = new ContinuousQueryAsyncHandle<int, int, string>(
            new DisposeSpy(), channel, () => "cursor-" + ++calls);

        Assert.AreEqual("cursor-1", handle.GetInitialQueryCursor());
        Assert.AreEqual("cursor-2", handle.GetInitialQueryCursor());
    }

    /// <summary>
    /// Tests that <see cref="ContinuousQueryAsyncHandle{TK,TV,TInitial}.DisposeAsync"/> stops the query and
    /// completes the channel.
    /// </summary>
    [Test]
    public async Task DisposeAsync_StopsQueryAndCompletesChannel()
    {
        var channel = Channel.CreateUnbounded<ICacheEntryEvent<int, int>>();
        var stopSpy = new DisposeSpy();
        var handle = new ContinuousQueryAsyncHandle<int, int, string>(stopSpy, channel, () => "cursor");

        await handle.DisposeAsync();

        Assert.AreEqual(1, stopSpy.DisposeCount);
        Assert.IsTrue(channel.Reader.Completion.IsCompleted);
    }

    /// <summary>
    /// Tests that stopping is idempotent: ending enumeration and then disposing (or vice versa) stops the
    /// underlying query exactly once.
    /// </summary>
    [Test]
    public async Task StopQuery_Idempotent_AcrossEnumerationAndDispose()
    {
        var channel = Channel.CreateUnbounded<ICacheEntryEvent<int, int>>();
        var stopSpy = new DisposeSpy();
        var handle = new ContinuousQueryAsyncHandle<int, int, string>(stopSpy, channel, () => "cursor");

        channel.Writer.TryWrite(Event(1));

        await foreach (var unused in handle.GetEvents())
        {
            break;
        }

        await handle.DisposeAsync();

        Assert.AreEqual(1, stopSpy.DisposeCount);
    }

    /// <summary>
    /// Tests that <see cref="ContinuousQueryAsync.CreateQuery{TK,TV}"/> maps all options onto the query.
    /// </summary>
    [Test]
    public void CreateQuery_AppliesOptions()
    {
        var channel = ContinuousQueryAsync.CreateChannel<int, int>();

        var options = new ContinuousQueryOptions
        {
            BufferSize = 42,
            TimeInterval = TimeSpan.FromSeconds(3),
            AutoUnsubscribe = false,
            Local = true,
            IncludeExpired = true
        };

        var qry = ContinuousQueryAsync.CreateQuery(channel.Writer, options, null);

        Assert.AreEqual(42, qry.BufferSize);
        Assert.AreEqual(TimeSpan.FromSeconds(3), qry.TimeInterval);
        Assert.IsFalse(qry.AutoUnsubscribe);
        Assert.IsTrue(qry.Local);
        Assert.IsTrue(qry.IncludeExpired);
        Assert.IsNull(qry.Filter);
        Assert.IsNotNull(qry.Listener);
    }

    /// <summary>
    /// Tests that <see cref="ContinuousQueryAsync.CreateQuery{TK,TV}"/> keeps the <see cref="ContinuousQuery"/>
    /// defaults when no options are provided.
    /// </summary>
    [Test]
    public void CreateQuery_NullOptions_UsesContinuousQueryDefaults()
    {
        var channel = ContinuousQueryAsync.CreateChannel<int, int>();

        var qry = ContinuousQueryAsync.CreateQuery(channel.Writer, null, null);

        Assert.AreEqual(ContinuousQuery.DefaultBufferSize, qry.BufferSize);
        Assert.AreEqual(ContinuousQuery.DefaultTimeInterval, qry.TimeInterval);
        Assert.AreEqual(ContinuousQuery.DefaultAutoUnsubscribe, qry.AutoUnsubscribe);
        Assert.IsFalse(qry.Local);
        Assert.IsFalse(qry.IncludeExpired);
    }

    /// <summary>
    /// Tests that the listener created by <see cref="ContinuousQueryAsync.CreateQuery{TK,TV}"/> forwards
    /// events into the channel.
    /// </summary>
    [Test]
    public void CreateQuery_ListenerForwardsEventsToChannel()
    {
        var channel = ContinuousQueryAsync.CreateChannel<int, int>();
        var qry = ContinuousQueryAsync.CreateQuery(channel.Writer, null, null);

        qry.Listener.OnEvent(new[] { Event(1), Event(2) });

        Assert.IsTrue(channel.Reader.TryRead(out var first));
        Assert.AreEqual(1, first.Key);
        Assert.IsTrue(channel.Reader.TryRead(out var second));
        Assert.AreEqual(2, second.Key);
        Assert.IsFalse(channel.Reader.TryRead(out _));
    }

    /// <summary>
    /// Tests that the filter created by <see cref="ContinuousQueryAsync.CreateQuery{TK,TV}"/> adapts an
    /// <see cref="ICacheEntryFilter{TK,TV}"/> into an event filter that delegates to <c>Invoke</c>.
    /// </summary>
    [Test]
    public void CreateQuery_WrapsEntryFilterAsEventFilter()
    {
        var channel = ContinuousQueryAsync.CreateChannel<int, int>();
        var filter = new RecordingFilter();

        var qry = ContinuousQueryAsync.CreateQuery(channel.Writer, null, filter);

        Assert.IsNotNull(qry.Filter);

        var evt = Event(5);

        filter.Result = true;
        Assert.IsTrue(qry.Filter.Evaluate(evt));

        filter.Result = false;
        Assert.IsFalse(qry.Filter.Evaluate(evt));

        Assert.AreEqual(2, filter.InvokeCount);
        Assert.AreSame(evt, filter.LastEntry);
    }

    /// <summary>
    /// Tests that <see cref="ContinuousQueryOptions"/> defaults match <see cref="ContinuousQuery"/> defaults,
    /// so that a freshly-constructed options object produces a valid query.
    /// </summary>
    [Test]
    public void ContinuousQueryOptions_Defaults_MatchContinuousQuery()
    {
        var options = new ContinuousQueryOptions();

        Assert.AreEqual(ContinuousQuery.DefaultBufferSize, options.BufferSize);
        Assert.AreEqual(ContinuousQuery.DefaultAutoUnsubscribe, options.AutoUnsubscribe);
        Assert.AreEqual(ContinuousQuery.DefaultTimeInterval, options.TimeInterval);
        Assert.IsFalse(options.Local);
        Assert.IsFalse(options.IncludeExpired);
    }

    /// <summary>
    /// Creates a cache entry event for the specified key (value equals the key).
    /// </summary>
    private static ICacheEntryEvent<int, int> Event(int key)
    {
        return new CacheEntryCreateEvent<int, int>(key, key);
    }

    /// <summary>
    /// <see cref="IDisposable"/> that counts how many times it was disposed.
    /// </summary>
    private sealed class DisposeSpy : IDisposable
    {
        public int DisposeCount { get; private set; }

        public void Dispose()
        {
            DisposeCount++;
        }
    }

    /// <summary>
    /// Cache entry filter that records invocations and returns a configurable result.
    /// </summary>
    private sealed class RecordingFilter : ICacheEntryFilter<int, int>
    {
        public bool Result { get; set; }

        public int InvokeCount { get; private set; }

        public ICacheEntry<int, int> LastEntry { get; private set; }

        public bool Invoke(ICacheEntry<int, int> entry)
        {
            InvokeCount++;
            LastEntry = entry;

            return Result;
        }
    }
}