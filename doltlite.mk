#
# doltlite.mk -- prolly engine and version-control layer build definitions.
#
# Kept out of main.mk so upstream SQLite changes to main.mk merge without
# stepping on doltlite's additions. Included from main.mk at the point
# where these lines append to LIBOBJS0 and OPT_FEATURE_FLAGS, so ordering
# is identical to having them inline.
#
#
# Prolly tree engine objects (when DOLTLITE_PROLLY=1)
#
#
# BLAKE3 SIMD object selection. The vendored BLAKE3 ships a runtime
# dispatcher that picks the best path on the current CPU; we only need
# to compile the SIMD source files that are valid for the target
# architecture. We probe via $(CC) (not $(B.cc)) because the wasm
# cross-compile path overrides only CC on the make command line, while
# B.cc stays bound to the host's cc from the autoconf-generated
# Makefile. Pass $(CFLAGS) into the probe too: an Apple-clang macOS
# cross-build keeps CC but selects the target arch via `-arch x86_64`
# in CFLAGS, and `cc -arch x86_64 -dumpmachine` reports the x86_64
# triple — without the flag the probe would report the host's arm64
# and wrongly pick blake3_neon.o.
#
BLAKE3_TARGET_TRIPLE := $(shell $(CC) $(CFLAGS) -dumpmachine 2>/dev/null)
ifneq (,$(findstring wasm,$(BLAKE3_TARGET_TRIPLE))$(findstring emscripten,$(BLAKE3_TARGET_TRIPLE)))
  # emcc/wasm32: SIMD intrinsics need -msimd128, which we don't want
  # to require. Stick to portable; dispatch.c compiles down to direct
  # portable calls when neither IS_X86 nor BLAKE3_USE_NEON is set.
  BLAKE3_SIMD_OBJS =
else ifneq (,$(filter x86_64% amd64% i686% i386%,$(BLAKE3_TARGET_TRIPLE)))
  BLAKE3_SIMD_OBJS = blake3_sse2.o blake3_sse41.o blake3_avx2.o blake3_avx512.o
else ifneq (,$(filter aarch64% arm64%,$(BLAKE3_TARGET_TRIPLE)))
  BLAKE3_SIMD_OBJS = blake3_neon.o
else
  BLAKE3_SIMD_OBJS =
endif

ED25519_SRC = fe.c ge.c sc.c sha512.c keypair.c sign.c verify.c
ED25519_OBJS = $(ED25519_SRC:%.c=ed25519_%.o)
MBEDTLS_SRC = $(notdir $(wildcard $(TOP)/ext/mbedtls/library/*.c))
MBEDTLS_OBJS = $(MBEDTLS_SRC:%.c=mbedtls_%.o)
# The credential/TLS stack (ed25519 + mbedtls) is built on every platform. On
# Windows, mbedtls's socket and entropy layers plus our own server sockets and
# BCryptGenRandom need ws2_32 and bcrypt linked (the MSVC #pragma comment(lib)
# in mbedtls doesn't apply under MinGW). Detect a Windows-esque build via the
# DLL suffix (autosetup resolves T.dll to .dll there) or the OS env var.
DOLTLITE_AUTH_OBJS = doltlite_creds.o doltlite_tls.o $(ED25519_OBJS) $(MBEDTLS_OBJS)
DOLTLITE_IS_WINDOWS := $(filter .dll,$(T.dll))$(filter Windows_NT,$(OS))
ifneq ($(DOLTLITE_IS_WINDOWS),)
  LDFLAGS.libsqlite3 += -lws2_32 -lbcrypt -lcrypt32
endif

PROLLY_OBJS = $(DOLTLITE_AUTH_OBJS) \
              prolly_hash.o prolly_xxhash.o blake3.o blake3_portable.o blake3_dispatch.o $(BLAKE3_SIMD_OBJS) prolly_hashset.o prolly_node.o prolly_cache.o \
              chunk_store.o chunk_store_lock.o chunk_store_refs_api.o chunk_store_commit.o chunk_wal.o chunk_refs.o chunk_index.o chunk_staging.o chunk_file.o prolly_cursor.o prolly_mutmap.o prolly_chunker.o \
              prolly_mutate.o prolly_check.o prolly_diff.o prolly_three_way_diff.o prolly_three_way_merge.o \
              prolly_btree.o prolly_btree_catalog.o prolly_btree_cursor.o prolly_btree_cursor_seek.o prolly_btree_cursor_payload.o prolly_btree_cursor_count.o prolly_btree_mutation.o \
              prolly_btree_orig.o prolly_btree_state.o prolly_btree_txn.o pager_shim.o sortkey.o \
              doltlite.o doltlite_core.o doltlite_cmd.o doltlite_add.o doltlite_commit_cmd.o doltlite_reset.o doltlite_merge_cmd.o doltlite_cherry_pick.o doltlite_revert.o doltlite_rebase.o doltlite_config.o doltlite_commit.o doltlite_ref.o doltlite_log.o doltlite_commit_ancestors.o doltlite_status.o doltlite_merge_status.o \
              doltlite_diff.o doltlite_diff_table.o doltlite_workspace.o doltlite_branch.o doltlite_branches.o doltlite_checkout.o doltlite_tag.o doltlite_ancestor.o doltlite_merge.o doltlite_merge_pass1.o doltlite_merge_pass2.o doltlite_merge_rows.o doltlite_merge_schema.o doltlite_conflicts.o \
              doltlite_gc.o doltlite_chunk_walk.o doltlite_history.o doltlite_at.o doltlite_blame.o doltlite_schema_diff.o doltlite_patch.o doltlite_schemas.o doltlite_diff_stat.o doltlite_record.o \
              doltlite_ignore.o doltlite_hashof.o \
              doltlite_constraint_violations.o doltlite_verify_constraints.o \
              doltlite_merge_constraints.o \
              doltlite_merge_constraints_unique.o \
              doltlite_merge_constraints_check.o \
              doltlite_merge_constraints_fk.o \
              doltlite_merge_constraints_notnull.o \
              doltlite_merge_constraints_strict.o \
              doltlite_dbpage.o \
              doltlite_remote.o doltlite_remote_sql.o \
              doltlite_http_remote.o doltlite_remotesrv.o

DOLTLITE_PROLLY ?= 1
DOLTLITE_VERSION ?= $(shell cat $(TOP)/.dolt_release_version 2>/dev/null || git describe --tags --always 2>/dev/null || echo "dev")
ifeq ($(DOLTLITE_PROLLY),1)
  # Replace btree.o/pager.o/wal.o/btmutex.o/backup.o with prolly engine
  LIBOBJS0 := $(filter-out btree.o pager.o wal.o btmutex.o backup.o,$(LIBOBJS0))
  LIBOBJS0 += $(PROLLY_OBJS)
  # Also compile original btree/pager/wal with renamed symbols for ATTACH
  LIBOBJS0 += btree_orig.o pager_orig.o wal_orig.o btmutex_orig.o backup_orig.o btree_orig_api.o
  OPT_FEATURE_FLAGS += -DDOLTLITE_PROLLY=1 -DDOLTLITE_VERSION='"$(DOLTLITE_VERSION)"'
  OPT_FEATURE_FLAGS += -DMBEDTLS_THREADING_C -DMBEDTLS_THREADING_PTHREAD
  # Generate a real doltlite amalgamation (prolly engine + VC layer woven in).
  AMALGAMATION_GEN_FLAGS += --doltlite
  # Source headers and the portable BLAKE3 sources that aren't in $(SRC) but the
  # amalgamation must inline; copied into tsrc by the .target_source rule.
  # Every doltlite/prolly/chunk source and header is inlined into the
  # amalgamation, so the list is globbed rather than hand-maintained: a
  # new source added to src/ is picked up automatically. A missed entry
  # used to leave the generated engine quietly short a file.
  #
  # The credential and TLS units are the exceptions: they compile
  # separately into DOLTLITE_AUTH_OBJS against the vendored ed25519 and
  # mbedtls include paths, and are linked rather than inlined.
  DOLTLITE_TSRC_EXCLUDE = \
    $(TOP)/src/doltlite_creds.c $(TOP)/src/doltlite_tls.c
  # Files the amalgamation needs that do not match those patterns.
  DOLTLITE_EXTRA_TSRC = \
    $(TOP)/src/record_codec.h \
    $(TOP)/src/btree_orig_prefix.h $(TOP)/src/btree_orig_api.h \
    $(TOP)/src/btree_orig_api.c \
    $(TOP)/ext/blake3/blake3.c $(TOP)/ext/blake3/blake3_portable.c \
    $(TOP)/ext/blake3/blake3_dispatch.c $(TOP)/ext/blake3/blake3.h \
    $(TOP)/ext/blake3/blake3_impl.h \
    $(filter-out $(DOLTLITE_TSRC_EXCLUDE), \
      $(wildcard $(TOP)/src/doltlite*.c) $(wildcard $(TOP)/src/doltlite*.h) \
      $(wildcard $(TOP)/src/prolly_*.c) $(wildcard $(TOP)/src/prolly_*.h) \
      $(wildcard $(TOP)/src/chunk_*.c) $(wildcard $(TOP)/src/chunk_*.h))
  ifeq ($(DOLTLITE_PROLLY_CHECK),1)
    OPT_FEATURE_FLAGS += -DDOLTLITE_PROLLY_CHECK=1
  endif
  # The non-amalgamation build compiles individual .c files into .o's, so
  # the SHELL_OPT flags the stock sqlite3 target passes at link time never
  # reach the preprocessor. That leaves dbpage.o, stmt.o, dbstat.o, rtree.o,
  # fts3*.o as empty translation units and (worse) strips the matching
  # entries out of main.o's sqlite3BuiltinExtensions[] — so sqlite_dbpage /
  # sqlite_stmt / rtree / fts4 / bytecode / dbstat never register on a
  # doltlite connection. Promote ONLY the vtable-enable macros into the
  # compile flags. Deliberately NOT propagating SHELL_OPT wholesale — it
  # also carries behavioral flags like SQLITE_DQS=0 and
  # SQLITE_STRICT_SUBTYPE=1 that would change semantics for code and
  # tests that were written assuming the doltlite default.
  OPT_FEATURE_FLAGS += \
    -DSQLITE_ENABLE_BYTECODE_VTAB \
    -DSQLITE_ENABLE_DBPAGE_VTAB \
    -DSQLITE_ENABLE_DBSTAT_VTAB \
    -DSQLITE_ENABLE_FTS4 \
    -DSQLITE_ENABLE_FTS5 \
    -DSQLITE_ENABLE_RTREE \
    -DSQLITE_ENABLE_STMTVTAB
endif
