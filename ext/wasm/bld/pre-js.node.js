/**
   BEGIN FILE: api/pre-js.js

   This file is intended to be prepended to the sqlite3.js build using
   Emscripten's --pre-js=THIS_FILE flag (or equivalent). It is run
   from inside of sqlite3InitModule(), after Emscripten's Module is
   defined, but early enough that we can ammend, or even outright
   replace, Module from here.

   Because this runs in-between Emscripten's own bootstrapping and
   Emscripten's main work, we must be careful with file-local symbol
   names. e.g. don't overwrite anything Emscripten defines and do not
   use 'const' for local symbols which Emscripten might try to use for
   itself. i.e. try to keep file-local symbol names obnoxiously
   collision-resistant.
*/
/**
   This file was preprocessed using:

   ./c-pp -o ./bld/pre-js.node.js -Dtarget:node -Dtarget:es6-module -Dtarget:es6-bundler-friendly -Dunsupported-build -DModule.instantiateWasm api/pre-js.c-pp.js
*/
/**
   UNSUPPORTED BUILD:

   This SQLite JS build configuration is entirely unsupported! It has
   not been tested beyond the ability to compile it. It may not
   load. It may not work properly. Only builds _directly_ targeting
   browser environments ("vanilla" JS and ESM modules) are supported
   and tested. Builds which _indirectly_ target browsers (namely
   bundler-friendly builds and any node builds) are not supported
   deliverables.
*/
/* END FILE: api/pre-js.js. */
