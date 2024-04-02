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

#include <time.h>
#include <vector>
#include <sstream>
#include <iomanip>

#define _WINSOCKAPI_
#include <windows.h>

#include <ignite/ignite_error.h>
#include <ignite/common/platform_utils.h>


/**
 * Convert standard WinAPI wide-char string to utf-8 string if possible.
 *
 * @param wtz Null terminated wide-char string.
 * @return UTF-8 string.
 */
std::string WcharToUtf8(const WCHAR *wtz)
{
    int len = WideCharToMultiByte(
            CP_UTF8,
            0,
            wtz,
            -1,
            NULL,
            0,
            NULL,
            NULL);

    if (len <= 0)
        return std::string();

    std::string utf8Tz;
    utf8Tz.resize(len);

    WideCharToMultiByte(
            CP_UTF8,
            0,
            wtz,
            -1,
            &utf8Tz[0],
            len,
            NULL,
            NULL);

    return utf8Tz;
}

// Original code is suggested by MSDN at
// https://docs.microsoft.com/en-us/windows/win32/sysinfo/converting-a-time-t-value-to-a-file-time
// Modified to fit larger time values
void TimeToFileTime(time_t tt, FILETIME& ft)
{
    ULARGE_INTEGER uli;
    uli.QuadPart = tt * 10000000 + 116444736000000000LL;

    ft.dwLowDateTime = uli.LowPart;
    ft.dwHighDateTime = uli.HighPart;
}

bool SystemTimeToTime(const SYSTEMTIME& sysTime, time_t& tt)
{
    FILETIME ft;
    if (!SystemTimeToFileTime(&sysTime, &ft))
        return false;

    ULARGE_INTEGER uli;
    uli.LowPart = ft.dwLowDateTime;
    uli.HighPart = ft.dwHighDateTime;

    long long sli = static_cast<long long>(uli.QuadPart);

    tt = static_cast<time_t>((sli - 116444736000000000LL) / 10000000);

    return true;
}

namespace ignite
{
    namespace common
    {
        time_t IgniteTimeGm(const tm& time)
        {
            SYSTEMTIME sysTime;
            memset(&sysTime, 0, sizeof(sysTime));

            sysTime.wYear = time.tm_year + 1900;
            sysTime.wMonth = time.tm_mon + 1;
            sysTime.wDay = time.tm_mday;
            sysTime.wHour = time.tm_hour;
            sysTime.wMinute = time.tm_min;
            sysTime.wSecond = time.tm_sec;

            time_t res;
            SystemTimeToTime(sysTime, res);

            return res;
        }

        bool IgniteGmTime(time_t in, tm& out)
        {
            FILETIME fileTime;
            TimeToFileTime(in, fileTime);

            SYSTEMTIME systemTime;
            if (!FileTimeToSystemTime(&fileTime, &systemTime))
                return false;

            out.tm_year = systemTime.wYear - 1900;
            out.tm_mon = systemTime.wMonth - 1;
            out.tm_mday = systemTime.wDay;
            out.tm_hour = systemTime.wHour;
            out.tm_min = systemTime.wMinute;
            out.tm_sec = systemTime.wSecond;

            return true;
        }

        std::string GetEnv(const std::string& name)
        {
            static const std::string empty;

            return GetEnv(name, empty);
        }

        std::string GetEnv(const std::string& name, const std::string& dflt)
        {
            char res[32767];

            DWORD envRes = GetEnvironmentVariableA(name.c_str(), res, sizeof(res) / sizeof(res[0]));

            if (envRes == 0 || envRes > sizeof(res))
                return dflt;

            return std::string(res, static_cast<size_t>(envRes));
        }

        bool FileExists(const std::string& path)
        {
            WIN32_FIND_DATAA findres;

            HANDLE hnd = FindFirstFileA(path.c_str(), &findres);

            if (hnd == INVALID_HANDLE_VALUE)
                return false;

            FindClose(hnd);

            return true;
        }

        bool IsValidDirectory(const std::string& path)
        {
            if (path.empty())
                return false;

            DWORD attrs = GetFileAttributesA(path.c_str());

            return attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY) != 0;
        }

        bool DeletePath(const std::string& path)
        {
            std::vector<TCHAR> path0(path.begin(), path.end());
            path0.push_back('\0');
            path0.push_back('\0');

            SHFILEOPSTRUCT fileop;
            fileop.hwnd = NULL;
            fileop.wFunc = FO_DELETE;
            fileop.pFrom = &path0[0];
            fileop.pTo = NULL;
            fileop.fFlags = FOF_NOCONFIRMATION | FOF_SILENT;

            fileop.fAnyOperationsAborted = FALSE;
            fileop.lpszProgressTitle = NULL;
            fileop.hNameMappings = NULL;

            int ret = SHFileOperation(&fileop);

            return ret == 0;
        }

        StdCharOutStream& Fs(StdCharOutStream& ostr)
        {
            ostr.put('\\');
            return ostr;
        }

        StdCharOutStream& Dle(StdCharOutStream& ostr)
        {
            static const char expansion[] = ".dll";

            ostr.write(expansion, sizeof(expansion) - 1);

            return ostr;
        }

        unsigned GetRandSeed()
        {
            return static_cast<unsigned>(GetTickCount() ^ GetCurrentProcessId());
        }

        std::string GetLastSystemError()
        {
            DWORD errorCode = GetLastError();

            std::string errorDetails;
            if (errorCode != ERROR_SUCCESS)
            {
                char errBuf[1024] = { 0 };

                FormatMessageA(
                        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, errorCode,
                        MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US), errBuf, sizeof(errBuf), NULL);

                errorDetails.assign(errBuf);
            }

            return errorDetails;
        }
    }
}
