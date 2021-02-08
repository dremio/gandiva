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

#include <time.h>

#include <gtest/gtest.h>
#include "precompiled/types.h"

namespace gandiva {

TEST(TestHash, TestHash32) {
  int8 s8 = 0;
  uint8 u8 = 0;
  int16 s16 = 0;
  uint16 u16 = 0;
  int32 s32 = 0;
  uint32 u32 = 0;
  int64 s64 = 0;
  uint64 u64 = 0;
  float32 f32 = 0;
  float64 f64 = 0;

  // hash of 0 should be non-zero (zero is the hash value for nulls).
  int32 zero_hash = hash32(s8, 0);
  EXPECT_NE(zero_hash, 0);

  // for a given value, all numeric types must have the same hash.
  EXPECT_EQ(hash32(u8, 0), zero_hash);
  EXPECT_EQ(hash32(s16, 0), zero_hash);
  EXPECT_EQ(hash32(u16, 0), zero_hash);
  EXPECT_EQ(hash32(s32, 0), zero_hash);
  EXPECT_EQ(hash32(u32, 0), zero_hash);
  EXPECT_EQ(hash32(s64, 0), zero_hash);
  EXPECT_EQ(hash32(u64, 0), zero_hash);
  EXPECT_EQ(hash32(f32, 0), zero_hash);
  EXPECT_EQ(hash32(f64, 0), zero_hash);

  // hash must change with a change in seed.
  EXPECT_NE(hash32(s8, 1), zero_hash);

  // for a given value and seed, all numeric types must have the same hash.
  EXPECT_EQ(hash32(s8, 1), hash32(s16, 1));
  EXPECT_EQ(hash32(s8, 1), hash32(u32, 1));
  EXPECT_EQ(hash32(s8, 1), hash32(f32, 1));
  EXPECT_EQ(hash32(s8, 1), hash32(f64, 1));
}

TEST(TestHash, TestHash64) {
  int8 s8 = 0;
  uint8 u8 = 0;
  int16 s16 = 0;
  uint16 u16 = 0;
  int32 s32 = 0;
  uint32 u32 = 0;
  int64 s64 = 0;
  uint64 u64 = 0;
  float32 f32 = 0;
  float64 f64 = 0;

  // hash of 0 should be non-zero (zero is the hash value for nulls).
  int64 zero_hash = hash64(s8, 0);
  EXPECT_NE(zero_hash, 0);
  EXPECT_NE(hash64(u8, 0), hash32(u8, 0));

  // for a given value, all numeric types must have the same hash.
  EXPECT_EQ(hash64(u8, 0), zero_hash);
  EXPECT_EQ(hash64(s16, 0), zero_hash);
  EXPECT_EQ(hash64(u16, 0), zero_hash);
  EXPECT_EQ(hash64(s32, 0), zero_hash);
  EXPECT_EQ(hash64(u32, 0), zero_hash);
  EXPECT_EQ(hash64(s64, 0), zero_hash);
  EXPECT_EQ(hash64(u64, 0), zero_hash);
  EXPECT_EQ(hash64(f32, 0), zero_hash);
  EXPECT_EQ(hash64(f64, 0), zero_hash);

  // hash must change with a change in seed.
  EXPECT_NE(hash64(s8, 1), zero_hash);

  // for a given value and seed, all numeric types must have the same hash.
  EXPECT_EQ(hash64(s8, 1), hash64(s16, 1));
  EXPECT_EQ(hash64(s8, 1), hash64(u32, 1));
  EXPECT_EQ(hash64(s8, 1), hash64(f32, 1));
}

TEST(TestHash, TestHashBuf) {
  const char *buf = "hello";
  int buf_len = 5;

  // hash should be non-zero (zero is the hash value for nulls).
  EXPECT_NE(hash32_buf((const uint8 *)buf, buf_len, 0), 0);
  EXPECT_NE(hash64_buf((const uint8 *)buf, buf_len, 0), 0);

  // hash must change if the string is changed.
  EXPECT_NE(hash32_buf((const uint8 *)buf, buf_len, 0),
            hash32_buf((const uint8 *)buf, buf_len - 1, 0));

  EXPECT_NE(hash64_buf((const uint8 *)buf, buf_len, 0),
            hash64_buf((const uint8 *)buf, buf_len - 1, 0));

  // hash must change if the seed is changed.
  EXPECT_NE(hash32_buf((const uint8 *)buf, buf_len, 0),
            hash32_buf((const uint8 *)buf, buf_len, 1));

  EXPECT_NE(hash64_buf((const uint8 *)buf, buf_len, 0),
            hash64_buf((const uint8 *)buf, buf_len, 1));
}

TEST(TestHash, TestHashSha256) {
  int8 s8 = 0;
  uint8 u8 = 0;
  int16 s16 = 0;
  uint16 u16 = 0;
  int32 s32 = 0;
  uint32 u32 = 0;
  int64 s64 = 0;
  uint64 u64 = 0;
  float32 f32 = 0;
  float64 f64 = 0;

  // hash of 0 should be non-zero (zero is the hash value for nulls).
  utf8 zero_hash = hash_sha256(s8);
  EXPECT_NE(zero_hash, "");

  // for a given value, all numeric types must have the same hash.
  EXPECT_STREQ(hash_sha256(u8), zero_hash);
  EXPECT_STREQ(hash_sha256(s16), zero_hash);
  EXPECT_STREQ(hash_sha256(u16), zero_hash);
  EXPECT_STREQ(hash_sha256(s32), zero_hash);
  EXPECT_STREQ(hash_sha256(u32), zero_hash);
  EXPECT_STREQ(hash_sha256(s64), zero_hash);
  EXPECT_STREQ(hash_sha256(u64), zero_hash);
  EXPECT_STREQ(hash_sha256(f32), zero_hash);
  EXPECT_STREQ(hash_sha256(f64), zero_hash);
}

TEST(TestHash, TestHashSha256Buf) {
  const char *buf = "hello";

  EXPECT_STRNE(hash_sha256_buf_op(buf), "");

  const char *modified_buf = "hEllo";

  EXPECT_STRNE(hash_sha256_buf_op(buf), hash_sha256_buf_op(modified_buf));
}

}  // namespace gandiva
