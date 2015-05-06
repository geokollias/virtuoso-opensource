/*
 *  snappy_2c.h
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
#ifndef SNAPPY_2C_H__
#define SNAPPY_2C_H__

#include <stddef.h>

#ifdef __cplusplus

extern "C" {
#endif

size_t snappy_max_compressed_length(size_t source_bytes);

int snappy_compress(const char* input,
    size_t input_length,
    char* compressed,
    size_t* compressed_length);

int snappy_uncompressed_length(const char* compressed, size_t compressed_length, size_t* uncompressed_length);

int snappy_uncompress(const char* compressed, size_t compressed_length, char* uncompressed);

#ifdef __cplusplus
}
#endif

#endif  /* SNAPPY_2C_H__ */
