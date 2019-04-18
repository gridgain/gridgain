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
 *
 * $Revision: 88625 $
*/

#ifndef H_TEAMCITY_MESSAGES
#define H_TEAMCITY_MESSAGES

#include <string>
#include <iostream>

namespace jetbrains { namespace teamcity {

std::string getFlowIdFromEnvironment();
bool underTeamcity();

class TeamcityMessages
{
    std::ostream* m_out;

public:
    static const bool StdErr = true;
    static const bool StdOut = false;

    TeamcityMessages();

    void setOutput(std::ostream&);

    void suiteStarted(const std::string& name, const std::string& flowid = std::string());
    void suiteFinished(const std::string& name, const std::string& flowid = std::string());

    void testStarted(const std::string& name, const std::string& flowid = std::string(), bool captureStandardOutput = false);
    void testFinished(const std::string& name, int durationMs = -1, const std::string& flowid = std::string());

    void testFailed(const std::string& name, const std::string& message, const std::string& details, const std::string& flowid = std::string());
    void testIgnored(const std::string& name, const std::string& message, const std::string& flowid = std::string());

    void testOutput(const std::string& name, const std::string& output, const std::string& flowid, bool isStdErr = StdOut);
};

}} // namespace teamcity, jetbrains

#endif /* H_TEAMCITY_MESSAGES */
