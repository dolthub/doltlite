
/* ^^^^ ACHTUNG: blank line at the start is necessary because
   Emscripten will not add a newline in some cases and we need
   a blank line for a sed-based kludge for the ES6 build.

   extern-post-js.js must be appended to the resulting sqlite3.js
   file. It gets its name from being used as the value for the
   --extern-post-js=... Emscripten flag. This code, unlike most of the
   associated JS code, runs outside of the Emscripten-generated module
   init scope, in the current global scope.

   At the time this is run, the global-scope sqlite3InitModule
   function will have just been defined.
*/
const toExportForESM =
(function(){
  //console.warn("this is extern-post-js");
  /**
     In order to hide the sqlite3InitModule()'s resulting
     Emscripten module from downstream clients (and simplify our
     documentation by being able to elide those details), we hide that
     function and expose a hand-written sqlite3InitModule() to return
     the sqlite3 object (most of the time).
  */
  const originalInit = sqlite3InitModule;
  if(!originalInit){
    throw new Error("Expecting sqlite3InitModule to be defined by the Emscripten build.");
  }
  /**
     We need to add some state which our custom Module.locateFile()
     can see, but an Emscripten limitation currently prevents us from
     attaching it to the sqlite3InitModule function object:

     https://github.com/emscripten-core/emscripten/issues/18071

     The only(?) current workaround is to temporarily stash this state
     into the global scope and delete it when sqlite3InitModule()
     is called.
  */
  const sIMS = globalThis.sqlite3InitModuleState = Object.assign(Object.create(null),{
    moduleScript: globalThis?.document?.currentScript,
    isWorker: ('undefined' !== typeof WorkerGlobalScope),
    location: globalThis.location,
    urlParams:  globalThis?.location?.href
      ? new URL(globalThis.location.href).searchParams
      : new URLSearchParams(),
    /*
      It is literally impossible to reliably get the name of _this_ script
      at runtime, so impossible to reliably derive X.wasm from script name
      X.js. (This is apparently why Emscripten hard-codes the name of the
      wasm file into their output.)  Thus we need, at build-time, to set
      the name of the WASM file which our custom instantiateWasm() should to
      load. The build process populates this.

      Module.instantiateWasm() is found in pre-js.c-pp.js.
    */
    wasmFilename: 'sqlite3.wasm' /* replaced by the build process */
  });
  sIMS.debugModule =
    sIMS.urlParams.has('sqlite3.debugModule')
    ? (...args)=>console.warn('sqlite3.debugModule:',...args)
    : ()=>{};

  if(sIMS.urlParams.has('sqlite3.dir')){
    sIMS.sqlite3Dir = sIMS.urlParams.get('sqlite3.dir') +'/';
  }else if(sIMS.moduleScript){
    const li = sIMS.moduleScript.src.split('/');
    li.pop();
    sIMS.sqlite3Dir = li.join('/') + '/';
  }

  const sIM = globalThis.sqlite3InitModule = function ff(...args){
    //console.warn("Using replaced sqlite3InitModule()",globalThis.location);
    sIMS.emscriptenLocateFile = args[0]?.locateFile /* see pre-js.c-pp.js [tag:locateFile] */;
    sIMS.emscriptenInstantiateWasm = args[0]?.instantiateWasm /* see pre-js.c-pp.js [tag:locateFile] */;
    return originalInit(...args).then((EmscriptenModule)=>{
      sIMS.debugModule("sqlite3InitModule() sIMS =",sIMS);
      sIMS.debugModule("sqlite3InitModule() EmscriptenModule =",EmscriptenModule);
      const s = EmscriptenModule.runSQLite3PostLoadInit(
        sIMS,
        EmscriptenModule /* see post-js-header/footer.js */,
        !!ff.__isUnderTest
      );
      sIMS.debugModule("sqlite3InitModule() sqlite3 =",s);
      //const rv = s.asyncPostInit();
      //delete s.asyncPostInit;
      return s;
    }).catch((e)=>{
      console.error("Exception loading sqlite3 module:",e);
      throw e;
    });
  };
  sIM.ready = originalInit.ready;

  if(sIMS.moduleScript){
    let src = sIMS.moduleScript.src.split('/');
    src.pop();
    sIMS.scriptDir = src.join('/') + '/';
  }
  sIMS.debugModule('extern-post-js.c-pp.js sqlite3InitModuleState =',sIMS);
  return sIM;
})();
sqlite3InitModule = toExportForESM;
export default sqlite3InitModule;
