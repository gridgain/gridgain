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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Log;

    public class ListLogger : ILogger
    {
        private readonly List<Entry> _entries = new List<Entry>();

        private readonly object _lock = new object();

        private readonly ILogger _wrappedLogger;

        public ListLogger(ILogger wrappedLogger = null)
        {
            _wrappedLogger = wrappedLogger;
        }

        public List<Entry> Entries
        {
            get
            {
                lock (_lock)
                {
                    return _entries.ToList();
                }
            }
        }

        public void Clear()
        {
            lock (_lock)
            {
                _entries.Clear();
            }
        }

        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category,
            string nativeErrorInfo, Exception ex)
        {
            if (_wrappedLogger != null)
            {
                _wrappedLogger.Log(level, message, args, formatProvider, category, nativeErrorInfo, ex);
            }
            
            lock (_lock)
            {
                if (args != null)
                {
                    message = string.Format(formatProvider, message, args);
                }
                
                _entries.Add(new Entry(message, level, category));
            }
        }

        public bool IsEnabled(LogLevel level)
        {
            return level == LogLevel.Debug;
        }

        public class Entry
        {
            private readonly string _message;
            private readonly LogLevel _level;
            private readonly string _category;

            public Entry(string message, LogLevel level, string category)
            {
                _message = message;
                _level = level;
                _category = category;
            }

            public string Message
            {
                get { return _message; }
            }

            public LogLevel Level
            {
                get { return _level; }
            }

            public string Category
            {
                get { return _category; }
            }
        }
    }
}
