/*
 *  snappy_2c.cc
 *
 *  C interface to the snappy library
 *
 *  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
 *  project.
 *
 *  Copyright (C) 1998-2014 OpenLink Software
 *
 *  This project is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the
 *  Free Software Foundation; only version 2 of the License, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "snappy_2c.h"

size_t BUF_SIZE=128*1024;
size_t BUF_SIZE2;
char* buffer;
char* buffer2;
char* buffer3;

int testBuf(size_t len1, char* fname) {
  size_t len2;
  size_t len3;
  int k;

  int err=snappy_compress(buffer, len1, buffer2, &len2);
  if (err) {
     printf(" error %d compressing file %s\n", err, fname);
     return err;
  }

  err=snappy_uncompressed_length(buffer2, len2, &len3);
  if (err) {
     printf(" error %d taking uncompressed length for file %s\n", err, fname);
     return 1;
  }
  if (len1!=len3) {
     printf(" file %s: uncompressed length=%d but %d expected\n", fname, (int)len3, (int)len1);
     return 1;
  }

  err=snappy_uncompress(buffer2, len2, buffer3);
  if (err) {
     printf(" error %d while uncompressing file %s\n", err, fname);
     return 1;
  }

  for(k=0; k<len1; k++) {
    if (buffer[k]!=buffer3[k]) {
      printf(" file %s: uncompressed is not equal source at:%d\n", fname, k);
    }
    return 99;
  }
  return 0;
}

int testFile(char* fname) {
  int res=0;
  size_t len1;
  int err;

  FILE *pFile;
  pFile = fopen(fname, "r");
  if (pFile==NULL) {
     printf("error opening file: %s\n", fname);
     return -1;
  }
  len1 = fread (buffer, 1, BUF_SIZE, pFile);
  err=ferror (pFile);
  if (err) {
     printf("error %d reading file: %s (len=%d)\n", err, fname, (int)len1);
     return err;
  }
//  printf("  len=%d\n", len);
  testBuf(len1, fname);
  fclose(pFile);
  printf("compress/uncompress passed for file:%s\n", fname);
  return res;
}

void allocBuf(char **buffer, size_t buf_size) {
    *buffer = (char*)malloc(sizeof (char) * buf_size);
    if (*buffer == NULL) {
     fputs ("Memory error",stderr);
     exit (2);
  }
}

int main(int argc, char** argv) {
  int res=0;
  int k;
  allocBuf(&buffer, BUF_SIZE);
  BUF_SIZE2=snappy_max_compressed_length(BUF_SIZE);
  allocBuf(&buffer2, BUF_SIZE2);
  allocBuf(&buffer3, BUF_SIZE);

  for (k = 1; k < argc; k++) {
    res+=testFile(argv[k]);
  }
  return res;
}
