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

namespace Apache.Ignite.Core.Tests.Log
{
    using Apache.Ignite.Core.Log;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ConsoleLogger"/>.
    /// </summary>
    public class ConsoleLoggerTest
    {
        /// <summary>
        /// Tests that default constructor sets log level to Warn.
        /// </summary>
        [Test]
        public void TestDefaultConstructorSetsWarnMinLevel()
        {
            Assert.AreEqual(LogLevel.Warn, new ConsoleLogger().MinLevel);
        }
        
        /// <summary>
        /// Tests that IsEnabled returns false when specified level is less that MinLevel.
        /// </summary>
        [Test]
        [TestCase(LogLevel.Error, LogLevel.Error, true)]
        [TestCase(LogLevel.Error, LogLevel.Warn, false)]
        [TestCase(LogLevel.Error, LogLevel.Info, false)]
        [TestCase(LogLevel.Error, LogLevel.Debug, false)]
        [TestCase(LogLevel.Error, LogLevel.Trace, false)]
        [TestCase(LogLevel.Warn, LogLevel.Error, true)]
        [TestCase(LogLevel.Warn, LogLevel.Warn, true)]
        [TestCase(LogLevel.Warn, LogLevel.Info, false)]
        [TestCase(LogLevel.Warn, LogLevel.Trace, false)]
        public void TestIsEnabledReturnsFalseWhenMinLevelIsGreaterThanLevel(
            LogLevel loggerLevel, LogLevel level, bool expectedResult)
        {
            var logger = new ConsoleLogger(loggerLevel);
            Assert.AreEqual(loggerLevel, logger.MinLevel);
            Assert.AreEqual(expectedResult, logger.IsEnabled(level));
        }

        /// <summary>
        /// Tests that logger writes to console.
        /// </summary>
        [Test]
        public void TestLogWritesToConsole()
        {
            
        }

        /// <summary>
        /// Tests that logger does not write to console when level is not enabled.
        /// </summary>
        [Test]
        public void TestLogDoesNotWriteToConsoleWhenLevelIsNotEnabled()
        {
            
        }
    }
}