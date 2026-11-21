#if defined(__GNUC__)
/* Vendored blake3 uses C99 mid-block declarations; keep its style. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeclaration-after-statement"
#endif
/* SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception OR CC0-1.0
**
** Vendored verbatim from https://github.com/BLAKE3-team/BLAKE3 (1.8.5).
** See ext/blake3/LICENSE and ext/blake3/README.md. */

#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>

#include "blake3.h"
#include "blake3_impl.h"

const char *blake3_version(void) { return BLAKE3_VERSION_STRING; }

INLINE void chunk_state_init(blake3_chunk_state *self, const uint32_t key[8],
                             uint8_t flags) {
  memcpy(self->cv, key, BLAKE3_KEY_LEN);
  self->chunk_counter = 0;
  memset(self->buf, 0, BLAKE3_BLOCK_LEN);
  self->buf_len = 0;
  self->blocks_compressed = 0;
  self->flags = flags;
}

INLINE void chunk_state_reset(blake3_chunk_state *self, const uint32_t key[8],
                              uint64_t chunk_counter) {
  memcpy(self->cv, key, BLAKE3_KEY_LEN);
  self->chunk_counter = chunk_counter;
  self->blocks_compressed = 0;
  memset(self->buf, 0, BLAKE3_BLOCK_LEN);
  self->buf_len = 0;
}

INLINE size_t chunk_state_len(const blake3_chunk_state *self) {
  return (BLAKE3_BLOCK_LEN * (size_t)self->blocks_compressed) +
         ((size_t)self->buf_len);
}

INLINE size_t chunk_state_fill_buf(blake3_chunk_state *self,
                                   const uint8_t *input, size_t input_len) {
  size_t take = BLAKE3_BLOCK_LEN - ((size_t)self->buf_len);
  if (take > input_len) {
    take = input_len;
  }
  uint8_t *dest = self->buf + ((size_t)self->buf_len);
  memcpy(dest, input, take);
  self->buf_len += (uint8_t)take;
  return take;
}

INLINE uint8_t chunk_state_maybe_start_flag(const blake3_chunk_state *self) {
  if (self->blocks_compressed == 0) {
    return CHUNK_START;
  } else {
    return 0;
  }
}

typedef struct {
  uint32_t input_cv[8];
  uint64_t counter;
  uint8_t block[BLAKE3_BLOCK_LEN];
  uint8_t block_len;
  uint8_t flags;
} output_t;

INLINE output_t make_output(const uint32_t input_cv[8],
                            const uint8_t block[BLAKE3_BLOCK_LEN],
                            uint8_t block_len, uint64_t counter,
                            uint8_t flags) {
  output_t ret;
  memcpy(ret.input_cv, input_cv, 32);
  memcpy(ret.block, block, BLAKE3_BLOCK_LEN);
  ret.block_len = block_len;
  ret.counter = counter;
  ret.flags = flags;
  return ret;
}

INLINE void output_chaining_value(const output_t *self, uint8_t cv[32]) {
  uint32_t cv_words[8];
  memcpy(cv_words, self->input_cv, 32);
  blake3_compress_in_place(cv_words, self->block, self->block_len,
                           self->counter, self->flags);
  store_cv_words(cv, cv_words);
}

INLINE void output_root_bytes(const output_t *self, uint64_t seek, uint8_t *out,
                              size_t out_len) {
  if (out_len == 0) {
      return;
  }
  uint64_t output_block_counter = seek / 64;
  size_t offset_within_block = seek % 64;
  uint8_t wide_buf[64];
  if(offset_within_block) {
    blake3_compress_xof(self->input_cv, self->block, self->block_len, output_block_counter, self->flags | ROOT, wide_buf);
    const size_t available_bytes = 64 - offset_within_block;
    const size_t bytes = out_len > available_bytes ? available_bytes : out_len;
    memcpy(out, wide_buf + offset_within_block, bytes);
    out += bytes;
    out_len -= bytes;
    output_block_counter += 1;
  }
  if(out_len / 64) {
    blake3_xof_many(self->input_cv, self->block, self->block_len, output_block_counter, self->flags | ROOT, out, out_len / 64);
  }
  output_block_counter += out_len / 64;
  out += out_len & -64;
  out_len -= out_len & -64;
  if(out_len) {
    blake3_compress_xof(self->input_cv, self->block, self->block_len, output_block_counter, self->flags | ROOT, wide_buf);
    memcpy(out, wide_buf, out_len);
  }
}

INLINE void chunk_state_update(blake3_chunk_state *self, const uint8_t *input,
                               size_t input_len) {
  if (self->buf_len > 0) {
    size_t take = chunk_state_fill_buf(self, input, input_len);
    input += take;
    input_len -= take;
    if (input_len > 0) {
      blake3_compress_in_place(
          self->cv, self->buf, BLAKE3_BLOCK_LEN, self->chunk_counter,
          self->flags | chunk_state_maybe_start_flag(self));
      self->blocks_compressed += 1;
      self->buf_len = 0;
      memset(self->buf, 0, BLAKE3_BLOCK_LEN);
    }
  }

  while (input_len > BLAKE3_BLOCK_LEN) {
    blake3_compress_in_place(self->cv, input, BLAKE3_BLOCK_LEN,
                             self->chunk_counter,
                             self->flags | chunk_state_maybe_start_flag(self));
    self->blocks_compressed += 1;
    input += BLAKE3_BLOCK_LEN;
    input_len -= BLAKE3_BLOCK_LEN;
  }

  chunk_state_fill_buf(self, input, input_len);
}

INLINE output_t chunk_state_output(const blake3_chunk_state *self) {
  uint8_t block_flags =
      self->flags | chunk_state_maybe_start_flag(self) | CHUNK_END;
  return make_output(self->cv, self->buf, self->buf_len, self->chunk_counter,
                     block_flags);
}

INLINE output_t parent_output(const uint8_t block[BLAKE3_BLOCK_LEN],
                              const uint32_t key[8], uint8_t flags) {
  return make_output(key, block, BLAKE3_BLOCK_LEN, 0, flags | PARENT);
}

INLINE size_t left_subtree_len(size_t input_len) {
  size_t full_chunks = (input_len - 1) / BLAKE3_CHUNK_LEN;
  return round_down_to_power_of_2(full_chunks) * BLAKE3_CHUNK_LEN;
}

/* DOLTLITE: a dummy chunk to round a hash_many() batch up to a lane count.
** Its chaining value lands in an output slot the caller never reads. */
static const uint8_t blake3_dl_pad_chunk[BLAKE3_CHUNK_LEN] = {0};

/* DOLTLITE: every hash_many() backend vectorises whole groups of its lane
** count and hashes the remainder one chunk at a time, so a 2- or 3-chunk
** batch never reaches the vector path at all. Rounding the batch up to the
** next lane count is at worst the cost of the serial remainder it replaces.
** Returns n unchanged when no backend can take it. */
INLINE size_t blake3_dl_padded_batch(size_t n) {
  size_t degree = blake3_simd_degree();
  size_t lanes = 4;
  if (n < 2 || degree < 4) {
    return n;
  }
  while (lanes < n) {
    lanes *= 2;
  }
  return lanes <= degree ? lanes : n;
}

INLINE size_t compress_chunks_parallel(const uint8_t *input, size_t input_len,
                                       const uint32_t key[8],
                                       uint64_t chunk_counter, uint8_t flags,
                                       uint8_t *out) {
  const uint8_t *chunks_array[MAX_SIMD_DEGREE];
  size_t input_position = 0;
  size_t chunks_array_len = 0;
  size_t batch_len;
  size_t i;
  while (input_len - input_position >= BLAKE3_CHUNK_LEN) {
    chunks_array[chunks_array_len] = &input[input_position];
    input_position += BLAKE3_CHUNK_LEN;
    chunks_array_len += 1;
  }

  batch_len = blake3_dl_padded_batch(chunks_array_len);
  for (i = chunks_array_len; i < batch_len; i++) {
    chunks_array[i] = blake3_dl_pad_chunk;
  }
  blake3_hash_many(chunks_array, batch_len,
                   BLAKE3_CHUNK_LEN / BLAKE3_BLOCK_LEN, key, chunk_counter,
                   true, flags, CHUNK_START, CHUNK_END, out);

  if (input_len > input_position) {
    uint64_t counter = chunk_counter + (uint64_t)chunks_array_len;
    blake3_chunk_state chunk_state;
    chunk_state_init(&chunk_state, key, flags);
    chunk_state.chunk_counter = counter;
    chunk_state_update(&chunk_state, &input[input_position],
                       input_len - input_position);
    output_t output = chunk_state_output(&chunk_state);
    output_chaining_value(&output, &out[chunks_array_len * BLAKE3_OUT_LEN]);
    return chunks_array_len + 1;
  } else {
    return chunks_array_len;
  }
}

INLINE size_t compress_parents_parallel(const uint8_t *child_chaining_values,
                                        size_t num_chaining_values,
                                        const uint32_t key[8], uint8_t flags,
                                        uint8_t *out) {
  const uint8_t *parents_array[MAX_SIMD_DEGREE_OR_2];
  size_t parents_array_len = 0;
  while (num_chaining_values - (2 * parents_array_len) >= 2) {
    parents_array[parents_array_len] =
        &child_chaining_values[2 * parents_array_len * BLAKE3_OUT_LEN];
    parents_array_len += 1;
  }

  blake3_hash_many(parents_array, parents_array_len, 1, key,
                   0,
                   false, flags | PARENT,
                   0,
                   0,
                   out);

  if (num_chaining_values > 2 * parents_array_len) {
    memcpy(&out[parents_array_len * BLAKE3_OUT_LEN],
           &child_chaining_values[2 * parents_array_len * BLAKE3_OUT_LEN],
           BLAKE3_OUT_LEN);
    return parents_array_len + 1;
  } else {
    return parents_array_len;
  }
}

size_t blake3_compress_subtree_wide(const uint8_t *input, size_t input_len,
                                    const uint32_t key[8],
                                    uint64_t chunk_counter, uint8_t flags,
                                    uint8_t *out, bool use_tbb) {
  (void)use_tbb;
  if (input_len <= blake3_simd_degree() * BLAKE3_CHUNK_LEN) {
    return compress_chunks_parallel(input, input_len, key, chunk_counter, flags,
                                    out);
  }

  size_t left_input_len = left_subtree_len(input_len);
  size_t right_input_len = input_len - left_input_len;
  const uint8_t *right_input = &input[left_input_len];
  uint64_t right_chunk_counter =
      chunk_counter + (uint64_t)(left_input_len / BLAKE3_CHUNK_LEN);

  uint8_t cv_array[2 * MAX_SIMD_DEGREE_OR_2 * BLAKE3_OUT_LEN];
  size_t degree = blake3_simd_degree();
  if (left_input_len > BLAKE3_CHUNK_LEN && degree == 1) {
    degree = 2;
  }
  uint8_t *right_cvs = &cv_array[degree * BLAKE3_OUT_LEN];

  size_t left_n = blake3_compress_subtree_wide(
      input, left_input_len, key, chunk_counter, flags, cv_array, use_tbb);
  size_t right_n = blake3_compress_subtree_wide(right_input, right_input_len, key,
                                         right_chunk_counter, flags, right_cvs,
                                         use_tbb);

  if (left_n == 1) {
    memcpy(out, cv_array, 2 * BLAKE3_OUT_LEN);
    return 2;
  }

  size_t num_chaining_values = left_n + right_n;
  return compress_parents_parallel(cv_array, num_chaining_values, key, flags,
                                   out);
}

INLINE void
compress_subtree_to_parent_node(const uint8_t *input, size_t input_len,
                                const uint32_t key[8], uint64_t chunk_counter,
                                uint8_t flags, uint8_t out[2 * BLAKE3_OUT_LEN],
                                bool use_tbb) {
  uint8_t cv_array[MAX_SIMD_DEGREE_OR_2 * BLAKE3_OUT_LEN];
  size_t num_cvs = blake3_compress_subtree_wide(input, input_len, key,
                                                chunk_counter, flags, cv_array, use_tbb);
  assert(num_cvs <= MAX_SIMD_DEGREE_OR_2);
#if MAX_SIMD_DEGREE_OR_2 > 2
  uint8_t out_array[MAX_SIMD_DEGREE_OR_2 * BLAKE3_OUT_LEN / 2];
  while (num_cvs > 2) {
    num_cvs =
        compress_parents_parallel(cv_array, num_cvs, key, flags, out_array);
    memcpy(cv_array, out_array, num_cvs * BLAKE3_OUT_LEN);
  }
#endif
  memcpy(out, cv_array, 2 * BLAKE3_OUT_LEN);
}

INLINE void hasher_init_base(blake3_hasher *self, const uint32_t key[8],
                             uint8_t flags) {
  memcpy(self->key, key, BLAKE3_KEY_LEN);
  chunk_state_init(&self->chunk, key, flags);
  self->cv_stack_len = 0;
}

void blake3_hasher_init(blake3_hasher *self) { hasher_init_base(self, IV, 0); }

void blake3_hasher_init_keyed(blake3_hasher *self,
                              const uint8_t key[BLAKE3_KEY_LEN]) {
  uint32_t key_words[8];
  load_key_words(key, key_words);
  hasher_init_base(self, key_words, KEYED_HASH);
}

void blake3_hasher_init_derive_key_raw(blake3_hasher *self, const void *context,
                                       size_t context_len) {
  blake3_hasher context_hasher;
  hasher_init_base(&context_hasher, IV, DERIVE_KEY_CONTEXT);
  blake3_hasher_update(&context_hasher, context, context_len);
  uint8_t context_key[BLAKE3_KEY_LEN];
  blake3_hasher_finalize(&context_hasher, context_key, BLAKE3_KEY_LEN);
  uint32_t context_key_words[8];
  load_key_words(context_key, context_key_words);
  hasher_init_base(self, context_key_words, DERIVE_KEY_MATERIAL);
}

void blake3_hasher_init_derive_key(blake3_hasher *self, const char *context) {
  blake3_hasher_init_derive_key_raw(self, context, strlen(context));
}

INLINE void hasher_merge_cv_stack(blake3_hasher *self, uint64_t total_len) {
  size_t post_merge_stack_len = (size_t)popcnt(total_len);
  while (self->cv_stack_len > post_merge_stack_len) {
    uint8_t *parent_node =
        &self->cv_stack[(self->cv_stack_len - 2) * BLAKE3_OUT_LEN];
    output_t output = parent_output(parent_node, self->key, self->chunk.flags);
    output_chaining_value(&output, parent_node);
    self->cv_stack_len -= 1;
  }
}

INLINE void hasher_push_cv(blake3_hasher *self, uint8_t new_cv[BLAKE3_OUT_LEN],
                           uint64_t chunk_counter) {
  hasher_merge_cv_stack(self, chunk_counter);
  memcpy(&self->cv_stack[self->cv_stack_len * BLAKE3_OUT_LEN], new_cv,
         BLAKE3_OUT_LEN);
  self->cv_stack_len += 1;
}

INLINE void blake3_hasher_update_base(blake3_hasher *self, const void *input,
                                      size_t input_len, bool use_tbb) {
  if (input_len == 0) {
    return;
  }

  const uint8_t *input_bytes = (const uint8_t *)input;

  if (chunk_state_len(&self->chunk) > 0) {
    size_t take = BLAKE3_CHUNK_LEN - chunk_state_len(&self->chunk);
    if (take > input_len) {
      take = input_len;
    }
    chunk_state_update(&self->chunk, input_bytes, take);
    input_bytes += take;
    input_len -= take;
    if (input_len > 0) {
      output_t output = chunk_state_output(&self->chunk);
      uint8_t chunk_cv[32];
      output_chaining_value(&output, chunk_cv);
      hasher_push_cv(self, chunk_cv, self->chunk.chunk_counter);
      chunk_state_reset(&self->chunk, self->key, self->chunk.chunk_counter + 1);
    } else {
      return;
    }
  }

  while (input_len > BLAKE3_CHUNK_LEN) {
    size_t subtree_len = round_down_to_power_of_2(input_len);
    uint64_t count_so_far = self->chunk.chunk_counter * BLAKE3_CHUNK_LEN;
    while ((((uint64_t)(subtree_len - 1)) & count_so_far) != 0) {
      subtree_len /= 2;
    }
    uint64_t subtree_chunks = subtree_len / BLAKE3_CHUNK_LEN;
    if (subtree_len <= BLAKE3_CHUNK_LEN) {
      blake3_chunk_state chunk_state;
      chunk_state_init(&chunk_state, self->key, self->chunk.flags);
      chunk_state.chunk_counter = self->chunk.chunk_counter;
      chunk_state_update(&chunk_state, input_bytes, subtree_len);
      output_t output = chunk_state_output(&chunk_state);
      uint8_t cv[BLAKE3_OUT_LEN];
      output_chaining_value(&output, cv);
      hasher_push_cv(self, cv, chunk_state.chunk_counter);
    } else {
      uint8_t cv_pair[2 * BLAKE3_OUT_LEN];
      compress_subtree_to_parent_node(input_bytes, subtree_len, self->key,
                                      self->chunk.chunk_counter,
                                      self->chunk.flags, cv_pair, use_tbb);
      hasher_push_cv(self, cv_pair, self->chunk.chunk_counter);
      hasher_push_cv(self, &cv_pair[BLAKE3_OUT_LEN],
                     self->chunk.chunk_counter + (subtree_chunks / 2));
    }
    self->chunk.chunk_counter += subtree_chunks;
    input_bytes += subtree_len;
    input_len -= subtree_len;
  }

  if (input_len > 0) {
    chunk_state_update(&self->chunk, input_bytes, input_len);
    hasher_merge_cv_stack(self, self->chunk.chunk_counter);
  }
}

void blake3_hasher_update(blake3_hasher *self, const void *input,
                          size_t input_len) {
  blake3_hasher_update_base(self, input, input_len, false);
}

void blake3_hasher_finalize(const blake3_hasher *self, uint8_t *out,
                            size_t out_len) {
  blake3_hasher_finalize_seek(self, 0, out, out_len);
}

void blake3_hasher_finalize_seek(const blake3_hasher *self, uint64_t seek,
                                 uint8_t *out, size_t out_len) {
  if (out_len == 0) {
    return;
  }

  if (self->cv_stack_len == 0) {
    output_t output = chunk_state_output(&self->chunk);
    output_root_bytes(&output, seek, out, out_len);
    return;
  }
  output_t output;
  size_t cvs_remaining;
  if (chunk_state_len(&self->chunk) > 0) {
    cvs_remaining = self->cv_stack_len;
    output = chunk_state_output(&self->chunk);
  } else {
    cvs_remaining = self->cv_stack_len - 2;
    output = parent_output(&self->cv_stack[cvs_remaining * 32], self->key,
                           self->chunk.flags);
  }
  while (cvs_remaining > 0) {
    cvs_remaining -= 1;
    uint8_t parent_block[BLAKE3_BLOCK_LEN];
    memcpy(parent_block, &self->cv_stack[cvs_remaining * 32], 32);
    output_chaining_value(&output, &parent_block[32]);
    output = parent_output(parent_block, self->key, self->chunk.flags);
  }
  output_root_bytes(&output, seek, out, out_len);
}

void blake3_hasher_reset(blake3_hasher *self) {
  chunk_state_reset(&self->chunk, self->key, 0);
  self->cv_stack_len = 0;
}

/* DOLTLITE: hash a whole buffer in one call. blake3_hasher_update() consumes
** input as a stream, so it must break a buffer into power-of-two aligned
** subtrees and compress them one after another; a 3712-byte prolly node
** becomes a 2-chunk subtree, then a 1-chunk subtree, then a partial chunk,
** none of which fill a SIMD batch. Handing the whole buffer to the wide
** compressor instead puts every full chunk in a single hash_many() call.
** Output is identical to init/update/finalize on the same bytes. */
void blake3_hash_oneshot(const void *input, size_t input_len, uint8_t *out,
                         size_t out_len) {
  output_t output;
  if (input_len <= BLAKE3_CHUNK_LEN) {
    blake3_chunk_state chunk;
    chunk_state_init(&chunk, IV, 0);
    chunk_state_update(&chunk, (const uint8_t *)input, input_len);
    output = chunk_state_output(&chunk);
  } else {
    uint8_t cv_pair[2 * BLAKE3_OUT_LEN];
    compress_subtree_to_parent_node((const uint8_t *)input, input_len, IV, 0, 0,
                                    cv_pair, false);
    output = parent_output(cv_pair, IV, 0);
  }
  output_root_bytes(&output, 0, out, out_len);
}
/* DOLTLITE: hash many buffers at once, 32-byte output each. A single prolly
** node has about four chunks, too few to fill an 8- or 16-lane SIMD batch, so
** the group is hashed column-wise instead: chunk j of every buffer goes
** through one hash_many() call, the trailing partial chunks are compressed
** four at a time, and each level of parent nodes across the group is another
** hash_many() call. Pairing adjacent chaining values and carrying an odd one
** up a level builds the same left-full tree the incremental hasher does, so
** output matches blake3_hash_oneshot(). */
#define DOLTLITE_MANY_GROUP 16
#define DOLTLITE_MANY_MAX_CHUNKS 16

typedef struct {
  const uint8_t *p;
  size_t len;
  uint64_t counter;
  uint8_t *out;
} doltlite_tail;

static void doltlite_tail_scalar(const doltlite_tail *t) {
  blake3_chunk_state chunk;
  output_t output;
  chunk_state_init(&chunk, IV, 0);
  chunk.chunk_counter = t->counter;
  chunk_state_update(&chunk, t->p, t->len);
  output = chunk_state_output(&chunk);
  output_chaining_value(&output, t->out);
}

#if defined(__GNUC__)
/* hash_many() starts every lane from one key and runs every lane for the
** same number of whole blocks, which a set of partial chunks does not offer.
** This compresses one block in each of four independent chunk states. */
typedef uint32_t doltlite_v4 __attribute__((vector_size(16)));

#define DOLTLITE_ROTR(x, c) (((x) >> (c)) | ((x) << (32 - (c))))
#define DOLTLITE_G(a, b, c, d, x, y)                                           \
  do {                                                                         \
    s[a] = s[a] + s[b] + (x);                                                  \
    s[d] = DOLTLITE_ROTR(s[d] ^ s[a], 16);                                     \
    s[c] = s[c] + s[d];                                                        \
    s[b] = DOLTLITE_ROTR(s[b] ^ s[c], 12);                                     \
    s[a] = s[a] + s[b] + (y);                                                  \
    s[d] = DOLTLITE_ROTR(s[d] ^ s[a], 8);                                      \
    s[c] = s[c] + s[d];                                                        \
    s[b] = DOLTLITE_ROTR(s[b] ^ s[c], 7);                                      \
  } while (0)

static void doltlite_compress4(doltlite_v4 cv[8], const uint8_t *const blk[4],
                               const uint32_t block_len[4],
                               const uint32_t counter[4],
                               const uint32_t flags[4],
                               const uint32_t mask[4]) {
  doltlite_v4 m[16];
  doltlite_v4 s[16];
  doltlite_v4 keep = (doltlite_v4){mask[0], mask[1], mask[2], mask[3]};
  size_t r, w;
  for (w = 0; w < 16; w++) {
    m[w] = (doltlite_v4){load32(blk[0] + 4 * w), load32(blk[1] + 4 * w),
                         load32(blk[2] + 4 * w), load32(blk[3] + 4 * w)};
  }
  for (w = 0; w < 8; w++) s[w] = cv[w];
  for (w = 0; w < 4; w++) s[8 + w] = (doltlite_v4){IV[w], IV[w], IV[w], IV[w]};
  s[12] = (doltlite_v4){counter[0], counter[1], counter[2], counter[3]};
  s[13] = (doltlite_v4){0, 0, 0, 0};
  s[14] = (doltlite_v4){block_len[0], block_len[1], block_len[2], block_len[3]};
  s[15] = (doltlite_v4){flags[0], flags[1], flags[2], flags[3]};
  for (r = 0; r < 7; r++) {
    const uint8_t *z = MSG_SCHEDULE[r];
    DOLTLITE_G(0, 4, 8, 12, m[z[0]], m[z[1]]);
    DOLTLITE_G(1, 5, 9, 13, m[z[2]], m[z[3]]);
    DOLTLITE_G(2, 6, 10, 14, m[z[4]], m[z[5]]);
    DOLTLITE_G(3, 7, 11, 15, m[z[6]], m[z[7]]);
    DOLTLITE_G(0, 5, 10, 15, m[z[8]], m[z[9]]);
    DOLTLITE_G(1, 6, 11, 12, m[z[10]], m[z[11]]);
    DOLTLITE_G(2, 7, 8, 13, m[z[12]], m[z[13]]);
    DOLTLITE_G(3, 4, 9, 14, m[z[14]], m[z[15]]);
  }
  for (w = 0; w < 8; w++) {
    cv[w] = ((s[w] ^ s[w + 8]) & keep) | (cv[w] & ~keep);
  }
}

/* a[] is ordered longest first, so a lane only ever idles after its own
** chunk has ended. An idle lane compresses zeros and keeps its value. */
static void doltlite_tails4(const doltlite_tail *a, size_t n) {
  static const uint8_t zeros[BLAKE3_BLOCK_LEN] = {0};
  uint8_t pad[4][BLAKE3_BLOCK_LEN];
  doltlite_v4 cv[8];
  size_t blocks[4];
  size_t b, l, w;
  for (w = 0; w < 8; w++) cv[w] = (doltlite_v4){IV[w], IV[w], IV[w], IV[w]};
  for (l = 0; l < 4; l++) {
    blocks[l] = l < n ? (a[l].len + BLAKE3_BLOCK_LEN - 1) / BLAKE3_BLOCK_LEN : 0;
  }
  for (b = 0; b < blocks[0]; b++) {
    const uint8_t *blk[4];
    uint32_t block_len[4], counter[4], flags[4], mask[4];
    for (l = 0; l < 4; l++) {
      size_t rem;
      if (b >= blocks[l]) {
        blk[l] = zeros;
        block_len[l] = 0;
        counter[l] = 0;
        flags[l] = 0;
        mask[l] = 0;
        continue;
      }
      rem = a[l].len - b * BLAKE3_BLOCK_LEN;
      if (rem >= BLAKE3_BLOCK_LEN) {
        blk[l] = a[l].p + b * BLAKE3_BLOCK_LEN;
        block_len[l] = BLAKE3_BLOCK_LEN;
      } else {
        memset(pad[l], 0, BLAKE3_BLOCK_LEN);
        memcpy(pad[l], a[l].p + b * BLAKE3_BLOCK_LEN, rem);
        blk[l] = pad[l];
        block_len[l] = (uint32_t)rem;
      }
      counter[l] = (uint32_t)a[l].counter;
      flags[l] = (b == 0 ? CHUNK_START : 0) | (b + 1 == blocks[l] ? CHUNK_END : 0);
      mask[l] = 0xffffffffu;
    }
    doltlite_compress4(cv, blk, block_len, counter, flags, mask);
  }
  for (l = 0; l < n; l++) {
    for (w = 0; w < 8; w++) store32(a[l].out + 4 * w, cv[w][l]);
  }
}
#endif

static void doltlite_hash_tails(doltlite_tail *a, size_t n) {
  size_t i = 0;
#if defined(__GNUC__)
  size_t j;
  for (i = 1; i < n; i++) {
    doltlite_tail t = a[i];
    for (j = i; j > 0 && a[j - 1].len < t.len; j--) a[j] = a[j - 1];
    a[j] = t;
  }
  for (i = 0; i + 1 < n; i += 4) {
    doltlite_tails4(&a[i], n - i < 4 ? n - i : 4);
  }
#endif
  for (; i < n; i++) doltlite_tail_scalar(&a[i]);
}

/* hash_many() vectorizes whole groups of four or more and compresses the
** stragglers one at a time. Repeating the first input to fill the last group
** costs nothing a SIMD lane was not already doing. */
static void doltlite_hash_batch(const uint8_t **batch, uint8_t **dest,
                                size_t k, size_t blocks, uint64_t counter,
                                uint8_t flags, uint8_t flags_start,
                                uint8_t flags_end) {
  uint8_t cvs[DOLTLITE_MANY_GROUP * BLAKE3_OUT_LEN];
  size_t padded = k;
  size_t i;
  while (padded & 3) batch[padded++] = batch[0];
  blake3_hash_many(batch, padded, blocks, IV, counter, false, flags,
                   flags_start, flags_end, cvs);
  for (i = 0; i < k; i++) {
    memcpy(dest[i], &cvs[i * BLAKE3_OUT_LEN], BLAKE3_OUT_LEN);
  }
}

static void doltlite_hash_group(const uint8_t *const *inputs,
                                const size_t *lens, size_t n, uint8_t *out) {
  uint8_t cv[DOLTLITE_MANY_GROUP][DOLTLITE_MANY_MAX_CHUNKS * BLAKE3_OUT_LEN];
  size_t count[DOLTLITE_MANY_GROUP];
  doltlite_tail tails[DOLTLITE_MANY_GROUP];
  const uint8_t *batch[DOLTLITE_MANY_GROUP];
  uint8_t *dest[DOLTLITE_MANY_GROUP];
  size_t max_len = 0;
  size_t n_tails = 0;
  size_t more = 1;
  size_t i, j, k;

  for (i = 0; i < n; i++) {
    count[i] = (lens[i] + BLAKE3_CHUNK_LEN - 1) / BLAKE3_CHUNK_LEN;
    if (lens[i] > max_len) max_len = lens[i];
  }
  for (j = 0; j * BLAKE3_CHUNK_LEN < max_len; j++) {
    size_t off = j * BLAKE3_CHUNK_LEN;
    k = 0;
    for (i = 0; i < n; i++) {
      if (off >= lens[i]) continue;
      if (lens[i] - off >= BLAKE3_CHUNK_LEN) {
        batch[k] = inputs[i] + off;
        dest[k++] = &cv[i][j * BLAKE3_OUT_LEN];
      } else {
        tails[n_tails].p = inputs[i] + off;
        tails[n_tails].len = lens[i] - off;
        tails[n_tails].counter = j;
        tails[n_tails++].out = &cv[i][j * BLAKE3_OUT_LEN];
      }
    }
    if (k > 0) {
      doltlite_hash_batch(batch, dest, k, BLAKE3_CHUNK_LEN / BLAKE3_BLOCK_LEN,
                          j, 0, CHUNK_START, CHUNK_END);
    }
  }
  doltlite_hash_tails(tails, n_tails);

  /* A parent's value lands in a slot at or below the pair it was read from,
  ** and a batch is read whole before any of it is written back. An odd
  ** value moves up only once its level is done, since the slot it moves to
  ** may be half of a pair still waiting in the batch. */
  while (more) {
    more = 0;
    k = 0;
    for (i = 0; i < n; i++) {
      size_t pairs = count[i] > 2 ? count[i] / 2 : 0;
      for (j = 0; j < pairs; j++) {
        batch[k] = &cv[i][2 * j * BLAKE3_OUT_LEN];
        dest[k++] = &cv[i][j * BLAKE3_OUT_LEN];
        if (k == DOLTLITE_MANY_GROUP) {
          doltlite_hash_batch(batch, dest, k, 1, 0, PARENT, 0, 0);
          k = 0;
        }
      }
    }
    if (k > 0) doltlite_hash_batch(batch, dest, k, 1, 0, PARENT, 0, 0);
    for (i = 0; i < n; i++) {
      size_t pairs = count[i] / 2;
      if (count[i] <= 2) continue;
      if (count[i] & 1) {
        memcpy(&cv[i][pairs * BLAKE3_OUT_LEN],
               &cv[i][(count[i] - 1) * BLAKE3_OUT_LEN], BLAKE3_OUT_LEN);
      }
      count[i] = pairs + (count[i] & 1);
      if (count[i] > 2) more = 1;
    }
  }
  for (i = 0; i < n; i++) {
    batch[i] = cv[i];
    dest[i] = out + i * BLAKE3_OUT_LEN;
  }
  doltlite_hash_batch(batch, dest, n, 1, 0, PARENT | ROOT, 0, 0);
}

void blake3_hash_oneshot_many(const void *const *inputs,
                              const size_t *input_lens, size_t n,
                              uint8_t *out) {
  const uint8_t *group[DOLTLITE_MANY_GROUP];
  size_t lens[DOLTLITE_MANY_GROUP];
  size_t at[DOLTLITE_MANY_GROUP];
  uint8_t hashes[DOLTLITE_MANY_GROUP * BLAKE3_OUT_LEN];
  size_t k = 0;
  size_t i, q;

  for (i = 0; i < n; i++) {
    size_t len = input_lens[i];
    if (len <= BLAKE3_CHUNK_LEN
     || len > DOLTLITE_MANY_MAX_CHUNKS * BLAKE3_CHUNK_LEN) {
      blake3_hash_oneshot(inputs[i], len, out + i * BLAKE3_OUT_LEN,
                          BLAKE3_OUT_LEN);
      continue;
    }
    group[k] = (const uint8_t *)inputs[i];
    lens[k] = len;
    at[k++] = i;
    if (k == DOLTLITE_MANY_GROUP) {
      doltlite_hash_group(group, lens, k, hashes);
      for (q = 0; q < k; q++) {
        memcpy(out + at[q] * BLAKE3_OUT_LEN, &hashes[q * BLAKE3_OUT_LEN],
               BLAKE3_OUT_LEN);
      }
      k = 0;
    }
  }
  if (k > 0) {
    doltlite_hash_group(group, lens, k, hashes);
    for (q = 0; q < k; q++) {
      memcpy(out + at[q] * BLAKE3_OUT_LEN, &hashes[q * BLAKE3_OUT_LEN],
             BLAKE3_OUT_LEN);
    }
  }
}
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
