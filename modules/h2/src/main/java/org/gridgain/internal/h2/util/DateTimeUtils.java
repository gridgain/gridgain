/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 * Iso8601: Initial Developer: Robert Rathsack (firstName dot lastName at gmx
 * dot de)
 */
package org.gridgain.internal.h2.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.gridgain.internal.h2.engine.Mode;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueDate;
import org.gridgain.internal.h2.value.ValueTime;
import org.gridgain.internal.h2.value.ValueTimestamp;
import org.gridgain.internal.h2.value.ValueTimestampTimeZone;

/**
 * This utility class contains time conversion functions.
 * <p>
 * Date value: a bit field with bits for the year, month, and day. Absolute day:
 * the day number (0 means 1970-01-01).
 */
public class DateTimeUtils {

    /**
     * The number of milliseconds per day.
     */
    public static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

    /**
     * The number of seconds per day.
     */
    public static final long SECONDS_PER_DAY = 24 * 60 * 60;

    /**
     * UTC time zone.
     */
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /**
     * The number of nanoseconds per second.
     */
    public static final long NANOS_PER_SECOND = 1_000_000_000;

    /**
     * The number of nanoseconds per minute.
     */
    public static final long NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND;

    /**
     * The number of nanoseconds per hour.
     */
    public static final long NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE;

    /**
     * The number of nanoseconds per day.
     */
    public static final long NANOS_PER_DAY = MILLIS_PER_DAY * 1_000_000;

    /**
     * The offset of year bits in date values.
     */
    static final int SHIFT_YEAR = 9;

    /**
     * The offset of month bits in date values.
     */
    static final int SHIFT_MONTH = 5;

    /**
     * Gregorian change date for a {@link GregorianCalendar} that represents a
     * proleptic Gregorian calendar.
     */
    private static final Date PROLEPTIC_GREGORIAN_CHANGE = new Date(Long.MIN_VALUE);

    /**
     * Date value for 1970-01-01.
     */
    public static final int EPOCH_DATE_VALUE = (1970 << SHIFT_YEAR) + (1 << SHIFT_MONTH) + 1;

    /**
     * Minimum possible date value.
     */
    public static final long MIN_DATE_VALUE = (-1_000_000_000L << SHIFT_YEAR) + (1 << SHIFT_MONTH) + 1;

    /**
     * Maximum possible date value.
     */
    public static final long MAX_DATE_VALUE = (1_000_000_000L << SHIFT_YEAR) + (12 << SHIFT_MONTH) + 31;

    private static final int[] NORMAL_DAYS_PER_MONTH = { 0, 31, 28, 31, 30, 31,
            30, 31, 31, 30, 31, 30, 31 };

    /**
     * Multipliers for {@link #convertScale(long, int)}.
     */
    private static final int[] CONVERT_SCALE_TABLE = { 1_000_000_000, 100_000_000,
            10_000_000, 1_000_000, 100_000, 10_000, 1_000, 100, 10 };

    private static volatile TimeZoneProvider LOCAL;

    /**
     * Raw offset doesn't change during DST transitions, but changes during
     * other transitions that some time zones have. H2 1.4.193 and later
     * versions use zone offset that is valid for startup time for performance
     * reasons. This code is now used only by old PageStore engine and its
     * datetime storage code has issues with all time zone transitions, so this
     * buggy logic is preserved as is too.
     */
    private static int zoneOffsetMillis = createGregorianCalendar().get(Calendar.ZONE_OFFSET);

    private DateTimeUtils() {
        // utility class
    }

    /**
     * Reset the cached calendar for default timezone, for example after
     * changing the default timezone.
     */
    public static void resetCalendar() {
        LOCAL = null;
        zoneOffsetMillis = createGregorianCalendar().get(Calendar.ZONE_OFFSET);
    }

    /**
     * Get the time zone provider for the default time zone.
     *
     * @return the time zone provider for the default time zone
     */
    public static TimeZoneProvider getTimeZone() {
        TimeZoneProvider local = LOCAL;
        if (local == null) {
            LOCAL = local = TimeZoneProvider.getDefault();
        }
        return local;
    }

    /**
     * Get a time zone provider for the given time zone.
     *
     * @param tz the time zone
     * @return a time zone provider for the given time zone
     */
    public static TimeZoneProvider getTimeZone(TimeZone tz) {
        return TimeZoneProvider.ofId(tz.getID());
    }

    /**
     * Creates a Gregorian calendar for the default timezone using the default
     * locale. Dates in H2 are represented in a Gregorian calendar. So this
     * method should be used instead of Calendar.getInstance() to ensure that
     * the Gregorian calendar is used for all date processing instead of a
     * default locale calendar that can be non-Gregorian in some locales.
     *
     * @return a new calendar instance.
     */
    public static GregorianCalendar createGregorianCalendar() {
        GregorianCalendar c = new GregorianCalendar();
        c.setGregorianChange(PROLEPTIC_GREGORIAN_CHANGE);
        return c;
    }

    /**
     * Creates a Gregorian calendar for the given timezone using the default
     * locale. Dates in H2 are represented in a Gregorian calendar. So this
     * method should be used instead of Calendar.getInstance() to ensure that
     * the Gregorian calendar is used for all date processing instead of a
     * default locale calendar that can be non-Gregorian in some locales.
     *
     * @param tz timezone for the calendar, is never null
     * @return a new calendar instance.
     */
    public static GregorianCalendar createGregorianCalendar(TimeZone tz) {
        GregorianCalendar c = new GregorianCalendar(tz);
        c.setGregorianChange(PROLEPTIC_GREGORIAN_CHANGE);
        return c;
    }

    /**
     * Convert a java.util.Date using the specified calendar.
     *
     * @param x the date
     * @param calendar the calendar
     * @return the date
     */
    public static ValueDate convertDate(Date x, Calendar calendar) {
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(x.getTime());
        long dateValue = dateValueFromCalendar(cal);
        return ValueDate.fromDateValue(dateValue);
    }

    /**
     * Convert the time using the specified calendar.
     *
     * @param x the time
     * @param calendar the calendar
     * @return the time
     */
    public static ValueTime convertTime(Time x, Calendar calendar) {
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(x.getTime());
        long nanos = nanosFromCalendar(cal);
        return ValueTime.fromNanos(nanos);
    }

    /**
     * Convert the timestamp using the specified calendar.
     *
     * @param x the time
     * @param calendar the calendar
     * @return the timestamp
     */
    public static ValueTimestamp convertTimestamp(Timestamp x,
            Calendar calendar) {
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(x.getTime());
        long dateValue = dateValueFromCalendar(cal);
        long nanos = nanosFromCalendar(cal);
        nanos += x.getNanos() % 1_000_000;
        return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
    }

    /**
     * Parse a date string. The format is: [+|-]year-month-day
     * or [+|-]yyyyMMdd.
     *
     * @param s the string to parse
     * @param start the parse index start
     * @param end the parse index end
     * @return the date value
     * @throws IllegalArgumentException if there is a problem
     */
    public static long parseDateValue(String s, int start, int end) {
        if (s.charAt(start) == '+') {
            // +year
            start++;
        }
        // start at position 1 to support "-year"
        int yEnd = s.indexOf('-', start + 1);
        int mStart, mEnd, dStart;
        if (yEnd > 0) {
            // Standard [+|-]year-month-day format
            mStart = yEnd + 1;
            mEnd = s.indexOf('-', mStart);
            if (mEnd <= mStart) {
                throw new IllegalArgumentException(s);
            }
            dStart = mEnd + 1;
        } else {
            // Additional [+|-]yyyyMMdd format for compatibility
            mEnd = dStart = end - 2;
            yEnd = mStart = mEnd - 2;
            // Accept only 3 or more digits in year for now
            if (yEnd < start + 3) {
                throw new IllegalArgumentException(s);
            }
        }
        int year = Integer.parseInt(s.substring(start, yEnd));
        int month = StringUtils.parseUInt31(s, mStart, mEnd);
        int day = StringUtils.parseUInt31(s, dStart, end);
        if (!isValidDate(year, month, day)) {
            throw new IllegalArgumentException(year + "-" + month + "-" + day);
        }
        return dateValue(year, month, day);
    }

    /**
     * Parse a time string. The format is: hour:minute[:second[.nanos]],
     * hhmm[ss[.nanos]], or hour.minute.second[.nanos].
     *
     * @param s the string to parse
     * @param start the parse index start
     * @param end the parse index end
     * @return the time in nanoseconds
     * @throws IllegalArgumentException if there is a problem
     */
    public static long parseTimeNanos(String s, int start, int end) {
        int hour, minute, second, nanos;
        int hEnd = s.indexOf(':', start);
        int mStart, mEnd, sStart, sEnd;
        if (hEnd > 0) {
            mStart = hEnd + 1;
            mEnd = s.indexOf(':', mStart);
            if (mEnd >= mStart) {
                // Standard hour:minute:second[.nanos] format
                sStart = mEnd + 1;
                sEnd = s.indexOf('.', sStart);
            } else {
                // Additional hour:minute format for compatibility
                mEnd = end;
                sStart = sEnd = -1;
            }
        } else {
            int t = s.indexOf('.', start);
            if (t < 0) {
                // Additional hhmm[ss] format for compatibility
                hEnd = mStart = start + 2;
                mEnd = mStart + 2;
                int len = end - start;
                if (len == 6) {
                    sStart = mEnd;
                    sEnd = -1;
                } else if (len == 4) {
                    sStart = sEnd = -1;
                } else {
                    throw new IllegalArgumentException(s);
                }
            } else if (t >= start + 6) {
                // Additional hhmmss.nanos format for compatibility
                if (t - start != 6) {
                    throw new IllegalArgumentException(s);
                }
                hEnd = mStart = start + 2;
                mEnd = sStart = mStart + 2;
                sEnd = t;
            } else {
                // Additional hour.minute.second[.nanos] IBM DB2 time format
                hEnd = t;
                mStart = hEnd + 1;
                mEnd = s.indexOf('.', mStart);
                if (mEnd <= mStart) {
                    throw new IllegalArgumentException(s);
                }
                sStart = mEnd + 1;
                sEnd = s.indexOf('.', sStart);
            }
        }
        hour = StringUtils.parseUInt31(s, start, hEnd);
        if (hour >= 24) {
            throw new IllegalArgumentException(s);
        }
        minute = StringUtils.parseUInt31(s, mStart, mEnd);
        if (sStart > 0) {
            if (sEnd < 0) {
                second = StringUtils.parseUInt31(s, sStart, end);
                nanos = 0;
            } else {
                second = StringUtils.parseUInt31(s, sStart, sEnd);
                nanos = parseNanos(s, sEnd + 1, end);
            }
        } else {
            second = nanos = 0;
        }
        if (minute >= 60 || second >= 60) {
            throw new IllegalArgumentException(s);
        }
        return ((((hour * 60L) + minute) * 60) + second) * NANOS_PER_SECOND + nanos;
    }

    /**
     * Parse nanoseconds.
     *
     * @param s String to parse.
     * @param start Begin position at the string to read.
     * @param end End position at the string to read.
     * @return Parsed nanoseconds.
     */
    static int parseNanos(String s, int start, int end) {
        if (start >= end) {
            throw new IllegalArgumentException(s);
        }
        int nanos = 0, mul = 100_000_000;
        do {
            char c = s.charAt(start);
            if (c < '0' || c > '9') {
                throw new IllegalArgumentException(s);
            }
            nanos += mul * (c - '0');
            // mul can become 0, but continue loop anyway to ensure that all
            // remaining digits are valid
            mul /= 10;
        } while (++start < end);
        return nanos;
    }

    /**
     * See:
     * https://stackoverflow.com/questions/3976616/how-to-find-nth-occurrence-of-character-in-a-string#answer-3976656
     */
    private static int findNthIndexOf(String str, char chr, int n) {
        int pos = str.indexOf(chr);
        while (--n > 0 && pos != -1) {
            pos = str.indexOf(chr, pos + 1);
        }
        return pos;
    }

    /**
     * Parses timestamp value from the specified string.
     *
     * @param s
     *            string to parse
     * @param mode
     *            database mode, or {@code null}
     * @param withTimeZone
     *            if {@code true} return {@link ValueTimestampTimeZone} instead of
     *            {@link ValueTimestamp}
     * @return parsed timestamp
     */
    public static Value parseTimestamp(String s, Mode mode, boolean withTimeZone) {
        int dateEnd = s.indexOf(' ');
        if (dateEnd < 0) {
            // ISO 8601 compatibility
            dateEnd = s.indexOf('T');
            if (dateEnd < 0 && mode != null && mode.allowDB2TimestampFormat) {
                // DB2 also allows dash between date and time
                dateEnd = findNthIndexOf(s, '-', 3);
            }
        }
        int timeStart;
        if (dateEnd < 0) {
            dateEnd = s.length();
            timeStart = -1;
        } else {
            timeStart = dateEnd + 1;
        }
        long dateValue = parseDateValue(s, 0, dateEnd);
        long nanos;
        int tzSeconds = 0;
        if (timeStart < 0) {
            nanos = 0;
        } else {
            int timeEnd = s.length();
            TimeZoneProvider tz = null;
            if (s.endsWith("Z")) {
                tz = TimeZoneProvider.UTC;
                timeEnd--;
            } else {
                int timeZoneStart = s.indexOf('+', dateEnd + 1);
                if (timeZoneStart < 0) {
                    timeZoneStart = s.indexOf('-', dateEnd + 1);
                }
                if (timeZoneStart >= 0) {
                    // Allow [timeZoneName] part after time zone offset
                    int offsetEnd = s.indexOf('[', timeZoneStart + 1);
                    if (offsetEnd < 0) {
                        offsetEnd = s.length();
                    }
                    String tzName = s.substring(timeZoneStart, offsetEnd);
                    tz = TimeZoneProvider.ofId(tzName);
                    if (s.charAt(timeZoneStart - 1) == ' ') {
                        timeZoneStart--;
                    }
                    timeEnd = timeZoneStart;
                } else {
                    timeZoneStart = s.indexOf(' ', dateEnd + 1);
                    if (timeZoneStart > 0) {
                        String tzName = s.substring(timeZoneStart + 1);
                        tz = TimeZoneProvider.ofId(tzName);
                        timeEnd = timeZoneStart;
                    }
                }
            }
            nanos = parseTimeNanos(s, dateEnd + 1, timeEnd);
            if (tz != null) {
                if (withTimeZone) {
                    if (tz != TimeZoneProvider.UTC) {
                        long seconds = tz.getEpochSecondsFromLocal(dateValue, nanos);
                        tzSeconds = tz.getTimeZoneOffsetUTC(seconds);
                    }
                } else {
                    long seconds = tz.getEpochSecondsFromLocal(dateValue, nanos);
                    seconds += getTimeZoneOffset(seconds);
                    dateValue = dateValueFromLocalSeconds(seconds);
                    nanos = nanos % 1_000_000_000 + nanosFromLocalSeconds(seconds);
                }
            }
        }
        if (withTimeZone) {
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, nanos, tzSeconds);
        }
        return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
    }

    /**
     * Calculates the time zone offset in seconds for the specified date
     * value, and nanoseconds since midnight.
     *
     * @param dateValue
     *            date value
     * @param timeNanos
     *            nanoseconds since midnight
     * @return time zone offset in seconds
     */
    public static int getTimeZoneOffset(long dateValue, long timeNanos) {
        return getTimeZone().getTimeZoneOffsetLocal(dateValue, timeNanos);
    }

    /**
     * Returns local time zone offset for a specified timestamp.
     *
     * @param ms milliseconds since Epoch in UTC
     * @return local time zone offset
     */
    public static int getTimeZoneOffsetMillis(long ms) {
        long seconds = ms / 1_000;
        // Round toward negative infinity
        if (ms < 0 && (seconds * 1_000 != ms)) {
            seconds--;
        }
        return getTimeZoneOffset(seconds) * 1_000;
    }

    /**
     * Returns local time zone offset for a specified EPOCH second.
     *
     * @param epochSeconds seconds since Epoch in UTC
     * @return local time zone offset in minutes
     */
    public static int getTimeZoneOffset(long epochSeconds) {
        return getTimeZone().getTimeZoneOffsetUTC(epochSeconds);
    }

    /**
     * Calculates the seconds since epoch for the specified date value,
     * nanoseconds since midnight, and time zone offset.
     * @param dateValue
     *            date value
     * @param timeNanos
     *            nanoseconds since midnight
     * @param offsetSeconds
     *            time zone offset in seconds
     * @return seconds since epoch in UTC
     */
    public static long getEpochSeconds(long dateValue, long timeNanos, int offsetSeconds) {
        return absoluteDayFromDateValue(dateValue) * SECONDS_PER_DAY + timeNanos / NANOS_PER_SECOND - offsetSeconds;
    }

    /**
     * Calculate the milliseconds since 1970-01-01 (UTC) for the given date and
     * time (in the specified timezone).
     *
     * @param tz the timezone of the parameters, or null for the default
     *            timezone
     * @param dateValue
     *            date value
     * @param timeNanos
     *            nanoseconds since midnight
     * @return the number of milliseconds (UTC)
     */
    public static long getMillis(TimeZone tz, long dateValue, long timeNanos) {
        TimeZoneProvider c = tz == null ? getTimeZone() : getTimeZone(tz);
        return c.getEpochSecondsFromLocal(dateValue, timeNanos) * 1_000 + timeNanos / 1_000_000 % 1_000;
    }

    /**
     * Extracts date value and nanos of day from the specified value.
     *
     * @param value
     *            value to extract fields from
     * @return array with date value and nanos of day
     */
    public static long[] dateAndTimeFromValue(Value value) {
        long dateValue = EPOCH_DATE_VALUE;
        long timeNanos = 0;
        if (value instanceof ValueTimestamp) {
            ValueTimestamp v = (ValueTimestamp) value;
            dateValue = v.getDateValue();
            timeNanos = v.getTimeNanos();
        } else if (value instanceof ValueDate) {
            dateValue = ((ValueDate) value).getDateValue();
        } else if (value instanceof ValueTime) {
            timeNanos = ((ValueTime) value).getNanos();
        } else if (value instanceof ValueTimestampTimeZone) {
            ValueTimestampTimeZone v = (ValueTimestampTimeZone) value;
            dateValue = v.getDateValue();
            timeNanos = v.getTimeNanos();
        } else {
            ValueTimestamp v = (ValueTimestamp) value.convertTo(Value.TIMESTAMP);
            dateValue = v.getDateValue();
            timeNanos = v.getTimeNanos();
        }
        return new long[] {dateValue, timeNanos};
    }

    /**
     * Creates a new date-time value with the same type as original value. If
     * original value is a ValueTimestampTimeZone, returned value will have the same
     * time zone offset as original value.
     *
     * @param original
     *            original value
     * @param dateValue
     *            date value for the returned value
     * @param timeNanos
     *            nanos of day for the returned value
     * @param forceTimestamp
     *            if {@code true} return ValueTimestamp if original argument is
     *            ValueDate or ValueTime
     * @return new value with specified date value and nanos of day
     */
    public static Value dateTimeToValue(Value original, long dateValue, long timeNanos, boolean forceTimestamp) {
        if (!(original instanceof ValueTimestamp)) {
            if (!forceTimestamp) {
                if (original instanceof ValueDate) {
                    return ValueDate.fromDateValue(dateValue);
                }
                if (original instanceof ValueTime) {
                    return ValueTime.fromNanos(timeNanos);
                }
            }
            if (original instanceof ValueTimestampTimeZone) {
                return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos,
                    ((ValueTimestampTimeZone) original).getTimeZoneOffsetSeconds());
            }
        }
        return ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);
    }

    /**
     * Get the number of milliseconds since 1970-01-01 in the local timezone,
     * but without daylight saving time into account.
     *
     * @param d the date
     * @return the milliseconds
     */
    public static long getTimeLocalWithoutDst(java.util.Date d) {
        return d.getTime() + zoneOffsetMillis;
    }

    /**
     * Convert the number of milliseconds since 1970-01-01 in the local timezone
     * to UTC, but without daylight saving time into account.
     *
     * @param millis the number of milliseconds in the local timezone
     * @return the number of milliseconds in UTC
     */
    public static long getTimeUTCWithoutDst(long millis) {
        return millis - zoneOffsetMillis;
    }

    /**
     * Returns day of week.
     *
     * @param dateValue
     *            the date value
     * @param firstDayOfWeek
     *            first day of week, Monday as 1, Sunday as 7 or 0
     * @return day of week
     * @see #getIsoDayOfWeek(long)
     */
    public static int getDayOfWeek(long dateValue, int firstDayOfWeek) {
        return getDayOfWeekFromAbsolute(absoluteDayFromDateValue(dateValue), firstDayOfWeek);
    }

    /**
     * Get the day of the week from the absolute day value.
     *
     * @param absoluteValue the absolute day
     * @param firstDayOfWeek the first day of the week
     * @return the day of week
     */
    public static int getDayOfWeekFromAbsolute(long absoluteValue, int firstDayOfWeek) {
        return absoluteValue >= 0 ? (int) ((absoluteValue - firstDayOfWeek + 11) % 7) + 1
                : (int) ((absoluteValue - firstDayOfWeek - 2) % 7) + 7;
    }

    /**
     * Returns number of day in year.
     *
     * @param dateValue
     *            the date value
     * @return number of day in year
     */
    public static int getDayOfYear(long dateValue) {
        int m = monthFromDateValue(dateValue);
        int a = (367 * m - 362) / 12 + dayFromDateValue(dateValue);
        if (m > 2) {
            a--;
            long y = yearFromDateValue(dateValue);
            if ((y & 3) != 0 || (y % 100 == 0 && y % 400 != 0)) {
                a--;
            }
        }
        return a;
    }

    /**
     * Returns ISO day of week.
     *
     * @param dateValue
     *            the date value
     * @return ISO day of week, Monday as 1 to Sunday as 7
     * @see #getSundayDayOfWeek(long)
     */
    public static int getIsoDayOfWeek(long dateValue) {
        return getDayOfWeek(dateValue, 1);
    }

    /**
     * Returns ISO number of week in year.
     *
     * @param dateValue
     *            the date value
     * @return number of week in year
     * @see #getIsoWeekYear(long)
     * @see #getWeekOfYear(long, int, int)
     */
    public static int getIsoWeekOfYear(long dateValue) {
        return getWeekOfYear(dateValue, 1, 4);
    }

    /**
     * Returns ISO week year.
     *
     * @param dateValue
     *            the date value
     * @return ISO week year
     * @see #getIsoWeekOfYear(long)
     * @see #getWeekYear(long, int, int)
     */
    public static int getIsoWeekYear(long dateValue) {
        return getWeekYear(dateValue, 1, 4);
    }

    /**
     * Returns day of week with Sunday as 1.
     *
     * @param dateValue
     *            the date value
     * @return day of week, Sunday as 1 to Monday as 7
     * @see #getIsoDayOfWeek(long)
     */
    public static int getSundayDayOfWeek(long dateValue) {
        return getDayOfWeek(dateValue, 0);
    }

    /**
     * Returns number of week in year.
     *
     * @param dateValue
     *            the date value
     * @param firstDayOfWeek
     *            first day of week, Monday as 1, Sunday as 7 or 0
     * @param minimalDaysInFirstWeek
     *            minimal days in first week of year
     * @return number of week in year
     * @see #getIsoWeekOfYear(long)
     */
    public static int getWeekOfYear(long dateValue, int firstDayOfWeek, int minimalDaysInFirstWeek) {
        long abs = absoluteDayFromDateValue(dateValue);
        int year = yearFromDateValue(dateValue);
        long base = getWeekOfYearBase(year, firstDayOfWeek, minimalDaysInFirstWeek);
        if (abs - base < 0) {
            base = getWeekOfYearBase(year - 1, firstDayOfWeek, minimalDaysInFirstWeek);
        } else if (monthFromDateValue(dateValue) == 12 && 24 + minimalDaysInFirstWeek < dayFromDateValue(dateValue)) {
            if (abs >= getWeekOfYearBase(year + 1, firstDayOfWeek, minimalDaysInFirstWeek)) {
                return 1;
            }
        }
        return (int) ((abs - base) / 7) + 1;
    }

    private static long getWeekOfYearBase(int year, int firstDayOfWeek, int minimalDaysInFirstWeek) {
        long first = absoluteDayFromYear(year);
        int daysInFirstWeek = 8 - getDayOfWeekFromAbsolute(first, firstDayOfWeek);
        long base = first + daysInFirstWeek;
        if (daysInFirstWeek >= minimalDaysInFirstWeek) {
            base -= 7;
        }
        return base;
    }

    /**
     * Returns week year.
     *
     * @param dateValue
     *            the date value
     * @param firstDayOfWeek
     *            first day of week, Monday as 1, Sunday as 7 or 0
     * @param minimalDaysInFirstWeek
     *            minimal days in first week of year
     * @return week year
     * @see #getIsoWeekYear(long)
     */
    public static int getWeekYear(long dateValue, int firstDayOfWeek, int minimalDaysInFirstWeek) {
        long abs = absoluteDayFromDateValue(dateValue);
        int year = yearFromDateValue(dateValue);
        long base = getWeekOfYearBase(year, firstDayOfWeek, minimalDaysInFirstWeek);
        if (abs - base < 0) {
            return year - 1;
        } else if (monthFromDateValue(dateValue) == 12 && 24 + minimalDaysInFirstWeek < dayFromDateValue(dateValue)) {
            if (abs >= getWeekOfYearBase(year + 1, firstDayOfWeek, minimalDaysInFirstWeek)) {
                return year + 1;
            }
        }
        return year;
    }

    /**
     * Returns number of days in month.
     *
     * @param year the year
     * @param month the month
     * @return number of days in the specified month
     */
    public static int getDaysInMonth(int year, int month) {
        if (month != 2) {
            return NORMAL_DAYS_PER_MONTH[month];
        }
        return (year & 3) == 0 && (year % 100 != 0 || year % 400 == 0) ? 29 : 28;
    }

    /**
     * Verify if the specified date is valid.
     *
     * @param year the year
     * @param month the month (January is 1)
     * @param day the day (1 is the first of the month)
     * @return true if it is valid
     */
    public static boolean isValidDate(int year, int month, int day) {
        return month >= 1 && month <= 12 && day >= 1 && day <= getDaysInMonth(year, month);
    }

    /**
     * Get the year from a date value.
     *
     * @param x the date value
     * @return the year
     */
    public static int yearFromDateValue(long x) {
        return (int) (x >>> SHIFT_YEAR);
    }

    /**
     * Get the month from a date value.
     *
     * @param x the date value
     * @return the month (1..12)
     */
    public static int monthFromDateValue(long x) {
        return (int) (x >>> SHIFT_MONTH) & 15;
    }

    /**
     * Get the day of month from a date value.
     *
     * @param x the date value
     * @return the day (1..31)
     */
    public static int dayFromDateValue(long x) {
        return (int) (x & 31);
    }

    /**
     * Get the date value from a given date.
     *
     * @param year the year
     * @param month the month (1..12)
     * @param day the day (1..31)
     * @return the date value
     */
    public static long dateValue(long year, int month, int day) {
        return (year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
    }

    /**
     * Get the date value from a given denormalized date with possible out of range
     * values of month and/or day. Used after addition or subtraction month or years
     * to (from) it to get a valid date.
     *
     * @param year
     *            the year
     * @param month
     *            the month, if out of range month and year will be normalized
     * @param day
     *            the day of the month, if out of range it will be saturated
     * @return the date value
     */
    public static long dateValueFromDenormalizedDate(long year, long month, int day) {
        long mm1 = month - 1;
        long yd = mm1 / 12;
        if (mm1 < 0 && yd * 12 != mm1) {
            yd--;
        }
        int y = (int) (year + yd);
        int m = (int) (month - yd * 12);
        if (day < 1) {
            day = 1;
        } else {
            int max = getDaysInMonth(y, m);
            if (day > max) {
                day = max;
            }
        }
        return dateValue(y, m, day);
    }

    /**
     * Convert a local seconds to an encoded date.
     *
     * @param localSeconds the seconds since 1970-01-01
     * @return the date value
     */
    public static long dateValueFromLocalSeconds(long localSeconds) {
        long absoluteDay = localSeconds / SECONDS_PER_DAY;
        // Round toward negative infinity
        if (localSeconds < 0 && (absoluteDay * SECONDS_PER_DAY != localSeconds)) {
            absoluteDay--;
        }
        return dateValueFromAbsoluteDay(absoluteDay);
    }

    /**
     * Convert a local datetime in millis to an encoded date.
     *
     * @param ms the milliseconds
     * @return the date value
     */
    public static long dateValueFromLocalMillis(long ms) {
        long absoluteDay = ms / MILLIS_PER_DAY;
        // Round toward negative infinity
        if (ms < 0 && (absoluteDay * MILLIS_PER_DAY != ms)) {
            absoluteDay--;
        }
        return dateValueFromAbsoluteDay(absoluteDay);
    }

    /**
     * Convert a time in seconds in local time to the nanoseconds since midnight.
     *
     * @param localSeconds the seconds since 1970-01-01
     * @return the nanoseconds
     */
    public static long nanosFromLocalSeconds(long localSeconds) {
        long absoluteDay = localSeconds / SECONDS_PER_DAY;
        // Round toward negative infinity
        if (localSeconds < 0 && (absoluteDay * SECONDS_PER_DAY != localSeconds)) {
            absoluteDay--;
        }
        return (localSeconds - absoluteDay * SECONDS_PER_DAY) * NANOS_PER_SECOND;
    }
    /**
     * Calculate the encoded date value from a given calendar.
     *
     * @param cal the calendar
     * @return the date value
     */
    private static long dateValueFromCalendar(Calendar cal) {
        int year = cal.get(Calendar.YEAR);
        if (cal.get(Calendar.ERA) == GregorianCalendar.BC) {
            year = 1 - year;
        }
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        return ((long) year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
    }

    /**
     * Convert a time in milliseconds in local time to the nanoseconds since midnight.
     *
     * @param ms the milliseconds
     * @return the nanoseconds
     */
    public static long nanosFromLocalMillis(long ms) {
        long absoluteDay = ms / MILLIS_PER_DAY;
        // Round toward negative infinity
        if (ms < 0 && (absoluteDay * MILLIS_PER_DAY != ms)) {
            absoluteDay--;
        }
        return (ms - absoluteDay * MILLIS_PER_DAY) * 1_000_000;
    }

    /**
     * Convert a java.util.Calendar to nanoseconds since midnight.
     *
     * @param cal the calendar
     * @return the nanoseconds
     */
    private static long nanosFromCalendar(Calendar cal) {
        int h = cal.get(Calendar.HOUR_OF_DAY);
        int m = cal.get(Calendar.MINUTE);
        int s = cal.get(Calendar.SECOND);
        int millis = cal.get(Calendar.MILLISECOND);
        return ((((((h * 60L) + m) * 60) + s) * 1000) + millis) * 1000000;
    }

    /**
     * Calculate the normalized timestamp.
     *
     * @param absoluteDay the absolute day
     * @param nanos the nanoseconds (may be negative or larger than one day)
     * @return the timestamp
     */
    public static ValueTimestamp normalizeTimestamp(long absoluteDay,
            long nanos) {
        if (nanos > NANOS_PER_DAY || nanos < 0) {
            long d;
            if (nanos > NANOS_PER_DAY) {
                d = nanos / NANOS_PER_DAY;
            } else {
                d = (nanos - NANOS_PER_DAY + 1) / NANOS_PER_DAY;
            }
            nanos -= d * NANOS_PER_DAY;
            absoluteDay += d;
        }
        return ValueTimestamp.fromDateValueAndNanos(
                dateValueFromAbsoluteDay(absoluteDay), nanos);
    }

    /**
     * Converts local date value and nanoseconds to timestamp with time zone.
     *
     * @param dateValue
     *            date value
     * @param timeNanos
     *            nanoseconds since midnight
     * @return timestamp with time zone
     */
    public static ValueTimestampTimeZone timestampTimeZoneFromLocalDateValueAndNanos(long dateValue, long timeNanos) {
        return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos,
            getTimeZoneOffset(dateValue, timeNanos));
    }

    /**
     * Creates the instance of the {@link ValueTimestampTimeZone} from milliseconds.
     *
     * @param ms milliseconds since 1970-01-01 (UTC)
     * @return timestamp with time zone with specified value and current time zone
     */
    public static ValueTimestampTimeZone timestampTimeZoneFromMillis(long ms) {
        int offset = getTimeZoneOffsetMillis(ms);
        ms += offset;
        long absoluteDay = ms / MILLIS_PER_DAY;
        // Round toward negative infinity
        if (ms < 0 && (absoluteDay * MILLIS_PER_DAY != ms)) {
            absoluteDay--;
        }
        return ValueTimestampTimeZone.fromDateValueAndNanos(
                dateValueFromAbsoluteDay(absoluteDay),
                (ms - absoluteDay * MILLIS_PER_DAY) * 1_000_000,
                offset / 1_000);
    }

    /**
     * Calculate the absolute day for a January, 1 of the specified year.
     *
     * @param year
     *            the year
     * @return the absolute day
     */
    public static long absoluteDayFromYear(long year) {
        long a = 365 * year - 719_528;
        if (year >= 0) {
            a += (year + 3) / 4 - (year + 99) / 100 + (year + 399) / 400;
        } else {
            a -= year / -4 - year / -100 + year / -400;
        }
        return a;
    }

    /**
     * Calculate the absolute day from an encoded date value.
     *
     * @param dateValue the date value
     * @return the absolute day
     */
    public static long absoluteDayFromDateValue(long dateValue) {
        return absoluteDay(yearFromDateValue(dateValue), monthFromDateValue(dateValue), dayFromDateValue(dateValue));
    }

    /**
     * Calculate the absolute day.
     *
     * @param y year
     * @param m month
     * @param d day
     * @return the absolute day
     */
    static long absoluteDay(long y, int m, int d) {
        long a = absoluteDayFromYear(y) + (367 * m - 362) / 12 + d - 1;
        if (m > 2) {
            a--;
            if ((y & 3) != 0 || (y % 100 == 0 && y % 400 != 0)) {
                a--;
            }
        }
        return a;
    }

    /**
     * Calculate the encoded date value from an absolute day.
     *
     * @param absoluteDay the absolute day
     * @return the date value
     */
    public static long dateValueFromAbsoluteDay(long absoluteDay) {
        long d = absoluteDay + 719_468;
        long a = 0;
        if (d < 0) {
            a = (d + 1) / 146_097 - 1;
            d -= a * 146_097;
            a *= 400;
        }
        long y = (400 * d + 591) / 146_097;
        int day = (int) (d - (365 * y + y / 4 - y / 100 + y / 400));
        if (day < 0) {
            y--;
            day = (int) (d - (365 * y + y / 4 - y / 100 + y / 400));
        }
        y += a;
        int m = (day * 5 + 2) / 153;
        day -= (m * 306 + 5) / 10 - 1;
        if (m >= 10) {
            y++;
            m -= 12;
        }
        return dateValue(y, m + 3, day);
    }

    /**
     * Return the next date value.
     *
     * @param dateValue
     *            the date value
     * @return the next date value
     */
    public static long incrementDateValue(long dateValue) {
        int day = dayFromDateValue(dateValue);
        if (day < 28) {
            return dateValue + 1;
        }
        int year = yearFromDateValue(dateValue);
        int month = monthFromDateValue(dateValue);
        if (day < getDaysInMonth(year, month)) {
            return dateValue + 1;
        }
        if (month < 12) {
            month++;
        } else {
            month = 1;
            year++;
        }
        return dateValue(year, month, 1);
    }

    /**
     * Return the previous date value.
     *
     * @param dateValue
     *            the date value
     * @return the previous date value
     */
    public static long decrementDateValue(long dateValue) {
        if (dayFromDateValue(dateValue) > 1) {
            return dateValue - 1;
        }
        int year = yearFromDateValue(dateValue);
        int month = monthFromDateValue(dateValue);
        if (month > 1) {
            month--;
        } else {
            month = 12;
            year--;
        }
        return dateValue(year, month, getDaysInMonth(year, month));
    }

    /**
     * Append a date to the string builder.
     *
     * @param buff the target string builder
     * @param dateValue the date value
     */
    public static void appendDate(StringBuilder buff, long dateValue) {
        int y = yearFromDateValue(dateValue);
        int m = monthFromDateValue(dateValue);
        int d = dayFromDateValue(dateValue);
        if (y > 0 && y < 10_000) {
            StringUtils.appendZeroPadded(buff, 4, y);
        } else {
            buff.append(y);
        }
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, d);
    }

    /**
     * Append a time to the string builder.
     *
     * @param buff the target string builder
     * @param nanos the time in nanoseconds
     */
    public static void appendTime(StringBuilder buff, long nanos) {
        if (nanos < 0) {
            buff.append('-');
            nanos = -nanos;
        }
        /*
         * nanos now either in range from 0 to Long.MAX_VALUE or equals to
         * Long.MIN_VALUE. We need to divide nanos by 1000000 with unsigned division to
         * get correct result. The simplest way to do this with such constraints is to
         * divide -nanos by -1000000.
         */
        long ms = -nanos / -1_000_000;
        nanos -= ms * 1_000_000;
        long s = ms / 1_000;
        ms -= s * 1_000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        StringUtils.appendZeroPadded(buff, 2, h);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, s);
        if (ms > 0 || nanos > 0) {
            buff.append('.');
            StringUtils.appendZeroPadded(buff, 3, ms);
            if (nanos > 0) {
                StringUtils.appendZeroPadded(buff, 6, nanos);
            }
            stripTrailingZeroes(buff);
        }
    }

    /**
     * Skip trailing zeroes.
     *
     * @param buff String buffer.
     */
    static void stripTrailingZeroes(StringBuilder buff) {
        int i = buff.length() - 1;
        if (buff.charAt(i) == '0') {
            while (buff.charAt(--i) == '0') {
                // do nothing
            }
            buff.setLength(i + 1);
        }
    }

    /**
     * Append a time zone to the string builder.
     *
     * @param buff the target string builder
     * @param tz the time zone offset in seconds
     */
    public static void appendTimeZone(StringBuilder buff, int tz) {
        if (tz < 0) {
            buff.append('-');
            tz = -tz;
        } else {
            buff.append('+');
        }
        int rem = tz / 3_600;
        StringUtils.appendZeroPadded(buff, 2, rem);
        tz -= rem * 3_600;
        if (tz != 0) {
            rem = tz / 60;
            buff.append(':');
            StringUtils.appendZeroPadded(buff, 2, rem);
            tz -= rem * 60;
            if (tz != 0) {
                buff.append(':');
                StringUtils.appendZeroPadded(buff, 2, tz);
            }
        }
    }

    /**
     * Formats timestamp with time zone as string.
     *
     * @param buff the target string builder
     * @param dateValue the year-month-day bit field
     * @param timeNanos nanoseconds since midnight
     * @param timeZoneOffsetSeconds the time zone offset in seconds
     */
    public static void appendTimestampTimeZone(StringBuilder buff, long dateValue, long timeNanos,
        int timeZoneOffsetSeconds) {
        appendDate(buff, dateValue);
        buff.append(' ');
        appendTime(buff, timeNanos);
        appendTimeZone(buff, timeZoneOffsetSeconds);
    }

    /**
     * Generates time zone name for the specified offset in seconds.
     *
     * @param offsetSeconds
     *            time zone offset in seconds
     * @return time zone name
     */
    public static String timeZoneNameFromOffsetSeconds(int offsetSeconds) {
        if (offsetSeconds == 0) {
            return "UTC";
        }
        StringBuilder b = new StringBuilder(12);
        b.append("GMT");
        if (offsetSeconds < 0) {
            b.append('-');
            offsetSeconds = -offsetSeconds;
        } else {
            b.append('+');
        }
        StringUtils.appendZeroPadded(b, 2, offsetSeconds / 3_600);
        b.append(':');
        offsetSeconds %= 3_600;
        StringUtils.appendZeroPadded(b, 2, offsetSeconds / 60);
        offsetSeconds %= 60;
        if (offsetSeconds != 0) {
            b.append(':');
            StringUtils.appendZeroPadded(b, 2, offsetSeconds);
        }
        return b.toString();
    }

    /**
     * Converts scale of nanoseconds.
     *
     * @param nanosOfDay nanoseconds of day
     * @param scale fractional seconds precision
     * @return scaled value
     */
    public static long convertScale(long nanosOfDay, int scale) {
        if (scale >= 9) {
            return nanosOfDay;
        }
        int m = CONVERT_SCALE_TABLE[scale];
        long mod = nanosOfDay % m;
        if (mod >= m >>> 1) {
            nanosOfDay += m;
        }
        return nanosOfDay - mod;
    }

    /**
     * Sets  time zone for the node that is used to convert date.
     * @param tz Server time zone is used to convert date.
     */
    public static void setTimeZone(TimeZone tz) {
        LOCAL = TimeZoneProvider.ofId(tz.getID());
    }
}
