#include "/etc/pliops/store_lib_expo.h"

#include <stdio.h>

int main(int argc, char* argv[]) {
  PLIOPS_DB_t pliopsDB = PLIOPS_OpenDB(0, NULL);
  PLIOPS_STATUS_et returnValue = PLIOPS_CloseDB(pliopsDB);
  if (returnValue != PLIOPS_STATUS_OK) {
    printf(" Pliops Failed To Close The Device\n");
    return -1;
  }
  return 0;
}
