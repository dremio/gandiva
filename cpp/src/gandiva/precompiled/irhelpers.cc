/*
 * All files that need to be pre-compiled to IR should be sourced into this file.
 */
#include <stdio.h>

extern "C" {

#include "types.h"
#include "bitmap.cc"
#include "arithmetic_ops.cc"
#include "time.cc"

int print_double(char *msg, double val) {
  return printf(msg, val);
}

int print_float(char *msg, float val) {
  return printf(msg, val);
}

} // extern "C"
