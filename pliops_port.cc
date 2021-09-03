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

#include "/etc/pliops/store_lib_expo.h"

#include <stdio.h>

namespace port {
PLIOPS_DB_t pliopsDB;
uint8_t instanceId;

bool PliopsOpenDB(uint8_t instance) {
  instanceId = instance;
  printf("Pilops Open DB - %d\n", instanceId);
  pliopsDB = PLIOPS_OpenDB(1, NULL);
  return true;
}

bool PliopsCloseDB(void) {
  PLIOPS_STATUS_et returnValue = PLIOPS_CloseDB(pliopsDB);
  printf("Pilops Close DB - %d\n", instanceId);
  if (returnValue != PLIOPS_STATUS_OK) {
    printf(" Pliops Failed To Close The Device\n");
    return false;
  }
  return true;
}

int PliopsPutCommand(const char *key_buffer, size_t key_size, const char *data,
                     const uint32_t data_length) {
  PLIOPS_STATUS_et returnValue;
  char *start = const_cast<char *>(key_buffer);
  memcpy(start, &instanceId, sizeof(instanceId));
  returnValue = PLIOPS_Put(pliopsDB, (void *) key_buffer, key_size, (void *) data,
                           data_length, false);
  return (int) returnValue;
}

int PliopsGetCommand(const char *key_buffer, size_t key_size, char *data,
                     const uint32_t data_length, uint32_t &objectSize) {
  PLIOPS_STATUS_et returnValue;
  char *start = const_cast<char *>(key_buffer);
  memcpy(start, &instanceId, sizeof(instanceId));
  objectSize = 0;
  returnValue = PLIOPS_Get(pliopsDB, (void *) key_buffer, key_size, (void *) data,
                           data_length, &objectSize);
  // free(data);
  return (int) returnValue;
}
}
