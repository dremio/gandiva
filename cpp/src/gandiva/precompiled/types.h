/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef PRECOMPILED_TYPES_H
#define PRECOMPILED_TYPES_H

#include <stdint.h>

/*
 * Use the same names as in arrow data types. Makes it easy to write pre-processor macros.
 */
typedef bool boolean;
typedef int32_t int32;
typedef int64_t int64;
typedef float float32;
typedef double float64;
typedef int64_t date;
typedef int64_t time64;
typedef int64_t timestamp64;

#endif //PRECOMPILED_TYPES_H
