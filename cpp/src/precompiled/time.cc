// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern "C" {

#include <stdlib.h>
#include <time.h>

#include "./types.h"

#define MILLIS_TO_SEC(millis) (millis / 1000)
#define MILLIS_TO_MINS(millis) ((millis) / (60 * 1000))
#define MILLIS_TO_HOUR(millis) ((millis) / (60 * 60 * 1000))
#define MINS_IN_HOUR 60
#define SECONDS_IN_MINUTE 60

// Expand inner macro for all date types.
#define DATE_TYPES(INNER) \
  INNER(date64)           \
  INNER(timestamp)

// Extract millennium
#define EXTRACT_MILLENNIUM(TYPE)                  \
  FORCE_INLINE                                    \
  int64 extractMillennium##_##TYPE(TYPE millis) { \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis);  \
    struct tm tm;                                 \
    gmtime_r(&tsec, &tm);                         \
    return (1900 + tm.tm_year - 1) / 1000 + 1;    \
  }

DATE_TYPES(EXTRACT_MILLENNIUM)

// Extract century
#define EXTRACT_CENTURY(TYPE)                    \
  FORCE_INLINE                                   \
  int64 extractCentury##_##TYPE(TYPE millis) {   \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return (1900 + tm.tm_year - 1) / 100 + 1;    \
  }

DATE_TYPES(EXTRACT_CENTURY)

// Extract  decade
#define EXTRACT_DECADE(TYPE)                     \
  FORCE_INLINE                                   \
  int64 extractDecade##_##TYPE(TYPE millis) {    \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return (1900 + tm.tm_year) / 10;             \
  }

DATE_TYPES(EXTRACT_DECADE)

// Extract  year.
#define EXTRACT_YEAR(TYPE)                       \
  FORCE_INLINE                                   \
  int64 extractYear##_##TYPE(TYPE millis) {      \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return 1900 + tm.tm_year;                    \
  }

DATE_TYPES(EXTRACT_YEAR)

#define EXTRACT_DOY(TYPE)                        \
  FORCE_INLINE                                   \
  int64 extractDoy##_##TYPE(TYPE millis) {       \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return 1 + tm.tm_yday;                       \
  }

DATE_TYPES(EXTRACT_DOY)

#define EXTRACT_QUARTER(TYPE)                    \
  FORCE_INLINE                                   \
  int64 extractQuarter##_##TYPE(TYPE millis) {   \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return tm.tm_mon / 3 + 1;                    \
  }

DATE_TYPES(EXTRACT_QUARTER)

#define EXTRACT_MONTH(TYPE)                      \
  FORCE_INLINE                                   \
  int64 extractMonth##_##TYPE(TYPE millis) {     \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return 1 + tm.tm_mon;                        \
  }

DATE_TYPES(EXTRACT_MONTH)

#define JAN1_WDAY(ptm) ((ptm->tm_wday - (ptm->tm_yday % 7) + 7) % 7)

bool IsLeapYear(int yy) {
  if ((yy % 4) != 0) {
    // not divisible by 4
    return false;
  }

  // yy = 4x
  if ((yy % 400) == 0) {
    // yy = 400x
    return true;
  }

  // yy = 4x, return true if yy != 100x
  return ((yy % 100) != 0);
}

// Day belongs to current year
// Note that tm_yday is 0 for Jan 1 (subtract 1 from day in the below examples)
//
// If Jan 1 is Mon, (ptm->tm_yday) / 7 + 1 (Jan 1->WK1, Jan 8->WK2, etc)
// If Jan 1 is Tues, (ptm->tm_yday + 1) / 7 + 1 (Jan 1->WK1, Jan 7->WK2, etc)
// If Jan 1 is Wed, (ptm->tm_yday + 2) / 7 + 1
// If Jan 1 is Thu, (ptm->tm_yday + 3) / 7 + 1
//
// If Jan 1 is Fri, Sat or Sun, the first few days belong to the previous year
// If Jan 1 is Fri, (ptm->tm_yday - 3) / 7 + 1 (Jan 4->WK1, Jan 11->WK2)
// If Jan 1 is Sat, (ptm->tm_yday - 2) / 7 + 1 (Jan 3->WK1, Jan 10->WK2)
// If Jan 1 is Sun, (ptm->tm_yday - 1) / 7 + 1 (Jan 2->WK1, Jan 9->WK2)
int weekOfCurrentYear(struct tm *ptm) {
  int jan1_wday = JAN1_WDAY(ptm);
  switch (jan1_wday) {
    // Monday
    case 1:
    // Tuesday
    case 2:
    // Wednesday
    case 3:
    // Thursday
    case 4: {
      return (ptm->tm_yday + jan1_wday - 1) / 7 + 1;
    }
    // Friday
    case 5:
    // Saturday
    case 6: {
      return (ptm->tm_yday - (8 - jan1_wday)) / 7 + 1;
    }
    // Sunday
    case 0: {
      return (ptm->tm_yday - 1) / 7 + 1;
    }
  }

  // cannot reach here
  // keep compiler happy
  return 0;
}

// Jan 1-3
// If Jan 1 is one of Mon, Tue, Wed, Thu - belongs to week of current year
// If Jan 1 is Fri/Sat/Sun - belongs to previous year
int getJanWeekOfYear(struct tm *ptm) {
  int jan1_wday = JAN1_WDAY(ptm);

  if ((jan1_wday >= 1) && (jan1_wday <= 4)) {
    // Jan 1-3 with the week belonging to this year
    return 1;
  }

  if (jan1_wday == 5) {
    // Jan 1 is a Fri
    // Jan 1-3 belong to previous year. Dec 31 of previous year same week # as Jan 1-3
    // previous year is a leap year:
    // Prev Jan 1 is a Wed. Jan 6th is Mon
    // Dec 31 - Jan 6 = 366 - 5 = 361
    // week from Jan 6 = (361 - 1) / 7 + 1 = 52
    // week # in previous year = 52 + 1 = 53
    //
    // previous year is not a leap year. Jan 1 is Thu. Jan 5th is Mon
    // Dec 31 - Jan 5 = 365 - 4 = 361
    // week from Jan 5 = (361 - 1) / 7 + 1 = 52
    // week # in previous year = 52 + 1 = 53
    return 53;
  }

  if (jan1_wday == 0) {
    // Jan 1 is a Sun
    if (ptm->tm_mday > 1) {
      // Jan 2 and 3 belong to current year
      return 1;
    }

    // day belongs to previous year. Same as Dec 31
    // Same as the case where Jan 1 is a Fri, except that previous year
    // does not have an extra week
    // Hence, return 52
    return 52;
  }

  // Jan 1 is a Sat
  // Jan 1-2 belong to previous year
  if (ptm->tm_mday == 3) {
    // Jan 3, return 1
    return 1;
  }

  // prev Jan 1 is leap year
  // prev Jan 1 is a Thu
  // return 53 (extra week)
  if (IsLeapYear(1900 + ptm->tm_year - 1)) {
    return 53;
  }

  // prev Jan 1 is not a leap year
  // prev Jan 1 is a Fri
  // return 52 (no extra week)
  return 52;
}

// Dec 29-31
int getDecWeekOfYear(struct tm *ptm) {
  int next_jan1_wday = (ptm->tm_wday + (31 - ptm->tm_mday) + 1) % 7;

  if (next_jan1_wday == 4) {
    // next Jan 1 is a Thu
    // day belongs to week 1 of next year
    return 1;
  }

  if (next_jan1_wday == 3) {
    // next Jan 1 is a Wed
    // Dec 31 and 30 belong to next year - return 1
    if (ptm->tm_mday != 29) {
      return 1;
    }

    // Dec 29 belongs to current year
    return weekOfCurrentYear(ptm);
  }

  if (next_jan1_wday == 2) {
    // next Jan 1 is a Tue
    // Dec 31 belongs to next year - return 1
    if (ptm->tm_mday == 31) {
      return 1;
    }

    // Dec 29 and 30 belong to current year
    return weekOfCurrentYear(ptm);
  }

  // next Jan 1 is a Fri/Sat/Sun. No day from this year belongs to that week
  // next Jan 1 is a Mon. No day from this year belongs to that week
  return weekOfCurrentYear(ptm);
}

// Week of year is determined by ISO 8601 standard
// Take a look at: https://en.wikipedia.org/wiki/ISO_week_date
//
// Important points to note:
// Week starts with a Monday and ends with a Sunday
// A week can have some days in this year and some days in the previous/next year
// This is true for the first and last weeks
//
// The first week of the year should have at-least 4 days in the current year
// The last week of the year should have at-least 4 days in the current year
//
// A given day might belong to the first week of the next year - e.g Dec 29, 30 and 31
// A given day might belong to the last week of the previous year - e.g. Jan 1, 2 and 3
//
// Algorithm:
// If day belongs to week in current year, weekOfCurrentYear
//
// If day is Jan 1-3, see getJanWeekOfYear
// If day is Dec 29-21, see getDecWeekOfYear
//
int64 weekOfYear(struct tm *ptm) {
  if ((ptm->tm_mon == 0) && (ptm->tm_mday <= 3)) {
    // Jan 1-3
    return getJanWeekOfYear(ptm);
  }

  if ((ptm->tm_mon == 11) && (ptm->tm_mday >= 29)) {
    // Dec 29-31
    return getDecWeekOfYear(ptm);
  }

  return weekOfCurrentYear(ptm);
}

#define EXTRACT_WEEK(TYPE)                       \
  FORCE_INLINE                                   \
  int64 extractWeek##_##TYPE(TYPE millis) {      \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return weekOfYear(&tm);                      \
  }

DATE_TYPES(EXTRACT_WEEK)

#define EXTRACT_DOW(TYPE)                        \
  FORCE_INLINE                                   \
  int64 extractDow##_##TYPE(TYPE millis) {       \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return 1 + tm.tm_wday;                       \
  }

DATE_TYPES(EXTRACT_DOW)

#define EXTRACT_DAY(TYPE)                        \
  FORCE_INLINE                                   \
  int64 extractDay##_##TYPE(TYPE millis) {       \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return tm.tm_mday;                           \
  }

DATE_TYPES(EXTRACT_DAY)

#define EXTRACT_HOUR(TYPE)                       \
  FORCE_INLINE                                   \
  int64 extractHour##_##TYPE(TYPE millis) {      \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return tm.tm_hour;                           \
  }

DATE_TYPES(EXTRACT_HOUR)

#define EXTRACT_MINUTE(TYPE)                     \
  FORCE_INLINE                                   \
  int64 extractMinute##_##TYPE(TYPE millis) {    \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return tm.tm_min;                            \
  }

DATE_TYPES(EXTRACT_MINUTE)

#define EXTRACT_SECOND(TYPE)                     \
  FORCE_INLINE                                   \
  int64 extractSecond##_##TYPE(TYPE millis) {    \
    time_t tsec = (time_t)MILLIS_TO_SEC(millis); \
    struct tm tm;                                \
    gmtime_r(&tsec, &tm);                        \
    return tm.tm_sec;                            \
  }

DATE_TYPES(EXTRACT_SECOND)

#define EXTRACT_EPOCH(TYPE) \
  FORCE_INLINE              \
  int64 extractEpoch##_##TYPE(TYPE millis) { return MILLIS_TO_SEC(millis); }

DATE_TYPES(EXTRACT_EPOCH)

// Functions that work on millis in a day
#define EXTRACT_SECOND_TIME(TYPE)                   \
  FORCE_INLINE                                      \
  int64 extractSecond##_##TYPE(TYPE millis) {       \
    int64 seconds_of_day = MILLIS_TO_SEC(millis);   \
    int64 sec = seconds_of_day % SECONDS_IN_MINUTE; \
    return sec;                                     \
  }

EXTRACT_SECOND_TIME(time32)

#define EXTRACT_MINUTE_TIME(TYPE)             \
  FORCE_INLINE                                \
  int64 extractMinute##_##TYPE(TYPE millis) { \
    TYPE mins = MILLIS_TO_MINS(millis);       \
    return (mins % (MINS_IN_HOUR));           \
  }

EXTRACT_MINUTE_TIME(time32)

#define EXTRACT_HOUR_TIME(TYPE) \
  FORCE_INLINE                  \
  int64 extractHour##_##TYPE(TYPE millis) { return MILLIS_TO_HOUR(millis); }

EXTRACT_HOUR_TIME(time32)

}  // extern "C"
