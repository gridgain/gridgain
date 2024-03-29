﻿/*
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

#if (!NETCOREAPP)
namespace Apache.Ignite.Core.Tests.Log
{
    using System.Collections.Generic;
    using System.Linq;
    using global::NLog;
    using global::NLog.Targets;

    /// <summary>
    /// NLog target which supports logging from multiple threads.
    /// </summary>
    public class ConcurrentMemoryTarget : TargetWithLayout
    {
        /// <summary> Object used for locking. </summary>
        private readonly object _locker = new object();

        /// <summary> Logs. </summary>
        private readonly IList<string> _logs;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrentMemoryTarget" /> class.
        /// </summary>
        public ConcurrentMemoryTarget()
        {
            _logs = new List<string>();
            Name = "ConcurrentMemoryTarget";
        }

        /// <summary>
        /// Gets the collection of logs gathered in the <see cref="ConcurrentMemoryTarget" />.
        /// </summary>
        public IEnumerable<string> Logs
        {
            get
            {
                lock (_locker)
                {
                    return _logs.ToList();
                }
            }
        }

        /// <summary>
        /// Renders the logging event message and adds it to the internal ArrayList of log messages.
        /// </summary>
        /// <param name="logEvent">The logging event.</param>
        protected override void Write(LogEventInfo logEvent)
        {
            lock (_locker)
            {
                var msg = Layout.Render(logEvent);

                _logs.Add(msg);
            }
        }
    }
}
#endif
