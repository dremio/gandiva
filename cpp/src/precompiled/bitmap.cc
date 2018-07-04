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

// BitMap functions

extern "C" {

#include "./types.h"

#define BITS_TO_BYTES(x) ((x + 7) / 8)
#define BITS_TO_WORDS(x) ((x + 63) / 64)

#define POS_TO_BYTE_INDEX(p) (p / 8)
#define POS_TO_BIT_INDEX(p) (p % 8)

FORCE_INLINE
bool bitMapGetBit(const unsigned char *bmap, int position) {
  int byteIdx = POS_TO_BYTE_INDEX(position);
  int bitIdx = POS_TO_BIT_INDEX(position);
  return ((bmap[byteIdx] & (1 << bitIdx)) > 0);
}

FORCE_INLINE
void bitMapSetBit(unsigned char *bmap, int position, bool value) {
  int byteIdx = POS_TO_BYTE_INDEX(position);
  int bitIdx = POS_TO_BIT_INDEX(position);
  if (value) {
    bmap[byteIdx] |= (1 << bitIdx);
  } else {
    bmap[byteIdx] &= ~(1 << bitIdx);
  }
}

// Clear the bit if value = false. Does nothing if value = true.
FORCE_INLINE
void bitMapClearBitIfFalse(unsigned char *bmap, int position, bool value) {
  if (!value) {
    int byteIdx = POS_TO_BYTE_INDEX(position);
    int bitIdx = POS_TO_BIT_INDEX(position);
    bmap[byteIdx] &= ~(1 << bitIdx);
  }
}

}  // extern "C"
