{
  "variables": {
    # Where to find libdoltlite.a + sqlite3.h. Defaults to the in-tree
    # build dir (../build); override with --doltlite_build=/path/to/build
    # or DOLTLITE_BUILD env var when consuming the published package.
    "doltlite_build%": "<!(node -e \"const p=require('path');console.log(process.env.DOLTLITE_BUILD||p.resolve(__dirname,'..','build'));\" 2>/dev/null || echo ../build)"
  },
  "targets": [
    {
      "target_name": "doltlite",
      "sources": [
        "src/binding.cc",
        "src/database.cc",
        "src/statement.cc"
      ],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")",
        "<(doltlite_build)"
      ],
      "libraries": [
        "<(doltlite_build)/libdoltlite.a",
        "-lz"
      ],
      "defines": [
        "NAPI_DISABLE_CPP_EXCEPTIONS",
        "NAPI_VERSION=8"
      ],
      "cflags_cc": ["-fno-exceptions", "-std=c++17"],
      "conditions": [
        ["OS=='mac'", {
          "xcode_settings": {
            "GCC_ENABLE_CPP_EXCEPTIONS": "NO",
            "CLANG_CXX_LANGUAGE_STANDARD": "c++17"
          }
        }],
        ["OS=='win'", {
          "msvs_settings": {
            "VCCLCompilerTool": {
              "ExceptionHandling": 0,
              "AdditionalOptions": ["/std:c++17"]
            }
          }
        }]
      ]
    }
  ]
}
