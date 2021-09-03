/*
 * Copyright (c) 2021 Triad National Security, LLC, as operator of Los Alamos
 * National Laboratory with the U.S. Department of Energy/National Nuclear
 * Security Administration. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "pliops_port.h"

#include <stdio.h>
#include <string>

namespace port {

std::string* AppendEscapedStringTo(std::string* str, const char* input,
                                   size_t n) {
  for (size_t i = 0; i < n; i++) {
    char c = input[i];
    if (c >= ' ' && c <= '~') {  // Readables are added as-is
      str->push_back(c);
    } else {
      char buf[10];
      snprintf(buf, sizeof(buf), "\\x%02x",
               static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
  return str;
}

bool PliopsOpenDB(uint8_t instance) {
  fprintf(stderr, "PLIOPS - OpenDB - %d\n", instance);
  return true;
}

bool PliopsCloseDB(void) {
  fprintf(stderr, "PLIOPS - Shutdown\n");
  return true;
}

int PliopsPutCommand(const char* key_buffer, size_t key_size, const char* data,
                     const uint32_t data_length) {
  std::string k;
  fprintf(stderr, "PLIOPS - PUT - k=%s/vlen=%u\n",
          AppendEscapedStringTo(&k, key_buffer, key_size)->c_str(),
          data_length);
  return 0;
}

int PliopsGetCommand(const char* key_buffer, size_t key_size, char* data,
                     const uint32_t data_length, uint32_t& objectSize) {
  std::string k;
  fprintf(stderr, "PLIOPS - GET - k=%s/vlen=16\n",
          AppendEscapedStringTo(&k, key_buffer, key_size)->c_str());
  objectSize = 16;
  return 0;
}

}  // namespace port
