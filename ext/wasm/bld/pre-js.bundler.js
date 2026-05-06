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

   ./c-pp -o ./bld/pre-js.bundler.js -Dtarget:es6-module -Dtarget:es6-bundler-friendly -DModule.instantiateWasm api/pre-js.c-pp.js
*/
/* END FILE: api/pre-js.js. */
