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

// String functions

extern "C" {

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "./types.h"

FORCE_INLINE
int32 octet_length_utf8(const utf8 input, int32 length) { return length; }

FORCE_INLINE
int32 bit_length_utf8(const utf8 input, int32 length) { return length * 8; }

FORCE_INLINE
int32 octet_length_binary(const binary input, int32 length) { return length; }

FORCE_INLINE
int32 bit_length_binary(const binary input, int32 length) { return length * 8; }

FORCE_INLINE
int32 mem_compare(const char *left, int32 left_len, const char *right, int32 right_len) {
  int min = left_len;
  if (right_len < min) {
    min = right_len;
  }

  int cmp_ret = memcmp(left, right, min);
  if (cmp_ret != 0) {
    return cmp_ret;
  } else {
    return left_len - right_len;
  }
}

// Expand inner macro for all varlen types.
#define VAR_LEN_OP_TYPES(INNER, NAME, OP) \
  INNER(NAME, utf8, OP)                   \
  INNER(NAME, binary, OP)

// Relational binary fns : left, right params are same, return is bool.
#define BINARY_RELATIONAL(NAME, TYPE, OP)                                        \
  FORCE_INLINE                                                                   \
  bool NAME##_##TYPE##_##TYPE(const TYPE left, int32 left_len, const TYPE right, \
                              int32 right_len) {                                 \
    return mem_compare(left, left_len, right, right_len) OP 0;                   \
  }

VAR_LEN_OP_TYPES(BINARY_RELATIONAL, equal, ==)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, not_equal, !=)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, less_than, <)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, less_than_or_equal_to, <=)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, greater_than, >)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, greater_than_or_equal_to, >=)

// Expand inner macro for all varlen types.
#define VAR_LEN_TYPES(INNER) \
  INNER(utf8)                \
  INNER(binary)

FORCE_INLINE
bool starts_with_utf8_utf8(const char *data, int32 data_len, const char *prefix,
                           int32 prefix_len) {
  return ((data_len >= prefix_len) && (memcmp(data, prefix, prefix_len) == 0));
}

FORCE_INLINE
bool ends_with_utf8_utf8(const char *data, int32 data_len, const char *suffix,
                         int32 suffix_len) {
  return ((data_len >= suffix_len) &&
          (memcmp(data + data_len - suffix_len, suffix, suffix_len) == 0));
}

FORCE_INLINE
int32 utf8_char_length(char c) {
  if (c >= 0) {  // 1-byte char
    return 1;
  } else if ((c & 0xE0) == 0xC0) {  // 2-byte char
    return 2;
  } else if ((c & 0xF0) == 0xE0) {  // 3-byte char
    return 3;
  } else if ((c & 0xF8) == 0xF0) {  // 4-byte char
    return 4;
  }
  // invalid char
  return 0;
}

FORCE_INLINE
void set_error_for_invalid_utf(int64_t execution_context, char val) {
  char const *fmt = "unexpected byte \\%02hhx encountered while decoding utf8 string";
  int size = strlen(fmt) + 64;
  char *error = (char *)malloc(size);
  snprintf(error, size, fmt, (unsigned char)val);
  context_set_error_msg(execution_context, error);
  free(error);
}

// Count the number of utf8 characters
FORCE_INLINE
int32 utf8_length(const char *data, int32 data_len, boolean is_valid, int64 context,
                  boolean *out_valid) {
  *out_valid = false;
  if (!is_valid) {
    return 0;
  }

  int char_len = 0;
  int count = 0;
  for (int i = 0; i < data_len; i += char_len) {
    char_len = utf8_char_length(data[i]);
    if (char_len == 0) {
      set_error_for_invalid_utf(context, data[i]);
      return 0;
    }
    ++count;
  }
  *out_valid = true;
  return count;
}

#define UTF8_LENGTH_NULL_INTERNAL(NAME, TYPE)                                 \
  FORCE_INLINE                                                                \
  int32 NAME##_##TYPE(TYPE in, int32 in_len, boolean is_valid, int64 context, \
                      boolean *out_valid) {                                   \
    return utf8_length(in, in_len, is_valid, context, out_valid);             \
  }

UTF8_LENGTH_NULL_INTERNAL(char_length, utf8)
UTF8_LENGTH_NULL_INTERNAL(length, utf8)
UTF8_LENGTH_NULL_INTERNAL(lengthUtf8, binary)

}  // extern "C"
