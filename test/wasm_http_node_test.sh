#!/usr/bin/env bash
set -euo pipefail

if ! command -v node >/dev/null 2>&1; then
  echo "SKIP: node not found"
  exit 0
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$REPO_ROOT/src/doltlite_http_remote.c"

node - "$SRC" <<'JS'
const assert = require("node:assert/strict");
const fs = require("node:fs");
const { Worker } = require("node:worker_threads");

const srcPath = process.argv[2];
const cSource = fs.readFileSync(srcPath, "utf8");

function extractEmJsBody(source, fnName) {
  const marker = `EM_JS(int, ${fnName},`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `missing ${marker}`);
  const paramsStart = source.indexOf("(", start + marker.length);
  assert.notEqual(paramsStart, -1, "missing param list");
  let depth = 0;
  let i = paramsStart;
  for (; i < source.length; i++) {
    if (source[i] === "(") depth++;
    else if (source[i] === ")") {
      depth--;
      if (depth === 0) break;
    }
  }
  const bodyStart = source.indexOf("{", i);
  assert.notEqual(bodyStart, -1, "missing body start");
  depth = 0;
  let inSingle = false;
  let inDouble = false;
  let escaped = false;
  let j = bodyStart;
  for (; j < source.length; j++) {
    const ch = source[j];
    if (escaped) {
      escaped = false;
      continue;
    }
    if ((inSingle || inDouble) && ch === "\\") {
      escaped = true;
      continue;
    }
    if (!inDouble && ch === "'") {
      inSingle = !inSingle;
      continue;
    }
    if (!inSingle && ch === '"') {
      inDouble = !inDouble;
      continue;
    }
    if (inSingle || inDouble) continue;
    if (ch === "{") depth++;
    else if (ch === "}") {
      depth--;
      if (depth === 0) break;
    }
  }
  assert.equal(depth, 0, "unbalanced braces in EM_JS body");
  return source.slice(bodyStart + 1, j);
}

const bodyJs = extractEmJsBody(cSource, "doltliteWasmHttpSend");
const makeSender = new Function(
  "Module",
  "UTF8ToString",
  "HEAPU8",
  "require",
  `return function doltliteWasmHttpSend(zUrl, zMethod, zAuth, pBody, nBody, timeoutMs) {${bodyJs}};`
);

function patternByte(idx, seed) {
  return (idx * 31 + seed) & 0xff;
}

function makePatternBuffer(len, seed) {
  const buf = Buffer.allocUnsafe(len);
  for (let i = 0; i < len; i++) buf[i] = patternByte(i, seed);
  return buf;
}

function checksum(buf) {
  let sum = 0;
  for (let i = 0; i < buf.length; i++) sum = (sum + buf[i]) >>> 0;
  return sum;
}

const serverWorkerSrc = `
const http = require("node:http");
const { parentPort } = require("node:worker_threads");

function makePatternBuffer(len, seed) {
  const buf = Buffer.allocUnsafe(len);
  for (let i = 0; i < len; i++) buf[i] = (i * 31 + seed) & 0xff;
  return buf;
}

const bigBuf = makePatternBuffer(9 * 1024 * 1024, 7);
const hugeBuf = makePatternBuffer(17 * 1024 * 1024, 13);
const counts = { ping: 0, echo: 0, big: 0, huge: 0 };

const server = http.createServer((req, res) => {
  if (req.url === "/ping" && req.method === "GET") {
    counts.ping++;
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("pong:" + counts.ping);
    return;
  }
  if (req.url === "/counts" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(counts));
    return;
  }
  if (req.url === "/echo" && req.method === "POST") {
    counts.echo++;
    let len = 0;
    let sum = 0;
    req.on("data", (chunk) => {
      len += chunk.length;
      for (let i = 0; i < chunk.length; i++) sum = (sum + chunk[i]) >>> 0;
    });
    req.on("end", () => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ len, sum, count: counts.echo }));
    });
    return;
  }
  if (req.url === "/big" && req.method === "GET") {
    counts.big++;
    res.writeHead(200, { "Content-Type": "application/octet-stream" });
    res.end(bigBuf);
    return;
  }
  if (req.url === "/huge" && req.method === "GET") {
    counts.huge++;
    res.writeHead(200, { "Content-Type": "application/octet-stream" });
    res.end(hugeBuf);
    return;
  }
  res.writeHead(404);
  res.end();
});

server.listen(0, "127.0.0.1", () => {
  parentPort.postMessage({ port: server.address().port });
});
`;

async function main() {
  const serverWorker = new Worker(serverWorkerSrc, { eval: true });
  const { port } = await new Promise((resolve, reject) => {
    serverWorker.once("message", resolve);
    serverWorker.once("error", reject);
  });
  const baseUrl = `http://127.0.0.1:${port}`;

  const Module = {};
  const strings = new Map();
  let nextStrPtr = 1;
  function allocStr(s) {
    const ptr = nextStrPtr++;
    strings.set(ptr, s);
    return ptr;
  }
  function UTF8ToString(ptr) {
    return strings.get(ptr) || "";
  }

  const uploadLen = 10 * 1024 * 1024;
  const uploadBuf = makePatternBuffer(uploadLen, 3);
  const uploadSum = checksum(uploadBuf);
  const bigExpectedSum = checksum(makePatternBuffer(9 * 1024 * 1024, 7));
  const hugeExpectedSum = checksum(makePatternBuffer(17 * 1024 * 1024, 13));

  const HEAPU8 = new Uint8Array(uploadLen + 64);
  HEAPU8.set(uploadBuf, 32);

  const send = makeSender(Module, UTF8ToString, HEAPU8, require);

  try {
    let status = send(allocStr(`${baseUrl}/ping`), allocStr("GET"), 0, 0, 0, 5000);
    assert.equal(status, 200);
    assert.equal(Buffer.from(Module.__doltliteHttp.resp).toString("utf8"), "pong:1");
    const nodeState = Module.__doltliteHttp.node;
    assert.equal(nodeState.req.byteLength, 8 * 1024 * 1024);
    assert.equal(nodeState.data.byteLength, 8 * 1024 * 1024);
    const ctlSab1 = nodeState.ctlSab;

    status = send(allocStr(`${baseUrl}/echo`), allocStr("POST"), 0, 32, uploadLen, 10000);
    assert.equal(status, 200, "upload > 8 MiB should succeed");
    const echoResp = JSON.parse(Buffer.from(Module.__doltliteHttp.resp).toString("utf8"));
    assert.equal(echoResp.len, uploadLen);
    assert.equal(echoResp.sum, uploadSum);
    assert.equal(echoResp.count, 1);
    assert.equal(nodeState.req.byteLength, 16 * 1024 * 1024);
    const ctlSab2 = nodeState.ctlSab;
    assert.notEqual(ctlSab2, ctlSab1, "req growth must allocate a fresh ctlSab");

    status = send(allocStr(`${baseUrl}/big`), allocStr("GET"), 0, 0, 0, 10000);
    assert.equal(status, 200, "response > 8 MiB should succeed");
    assert.equal(Module.__doltliteHttp.resp.length, 9 * 1024 * 1024);
    assert.equal(checksum(Module.__doltliteHttp.resp), bigExpectedSum);
    assert.equal(nodeState.data.byteLength, 16 * 1024 * 1024);
    const ctlSab3 = nodeState.ctlSab;
    assert.notEqual(ctlSab3, ctlSab2, "response overflow must allocate a fresh ctlSab");

    status = send(allocStr(`${baseUrl}/ping`), allocStr("GET"), 0, 0, 0, 5000);
    assert.equal(status, 200);
    assert.equal(Buffer.from(Module.__doltliteHttp.resp).toString("utf8"), "pong:2");

    status = send(allocStr(`${baseUrl}/huge`), allocStr("GET"), 0, 0, 0, 10000);
    assert.equal(status, 200, "response > 16 MiB should succeed");
    assert.equal(Module.__doltliteHttp.resp.length, 17 * 1024 * 1024);
    assert.equal(checksum(Module.__doltliteHttp.resp), hugeExpectedSum);
    assert.equal(nodeState.data.byteLength, 32 * 1024 * 1024);
    const ctlSab4 = nodeState.ctlSab;
    assert.notEqual(ctlSab4, ctlSab3, "second response overflow must allocate a fresh ctlSab");

    status = send(allocStr(`${baseUrl}/ping`), allocStr("GET"), 0, 0, 0, 5000);
    assert.equal(status, 200);
    assert.equal(Buffer.from(Module.__doltliteHttp.resp).toString("utf8"), "pong:3");

    status = send(allocStr(`${baseUrl}/counts`), allocStr("GET"), 0, 0, 0, 5000);
    assert.equal(status, 200);
    const counts = JSON.parse(Buffer.from(Module.__doltliteHttp.resp).toString("utf8"));
    assert.equal(counts.ping, 3);
    assert.equal(counts.echo, 1);

    console.log("PASS: wasm Node HTTP client grows req > 8 MiB and isolates ctlSab on respawn");
  } finally {
    if (Module.__doltliteHttp && Module.__doltliteHttp.node && Module.__doltliteHttp.node.worker) {
      await Module.__doltliteHttp.node.worker.terminate();
    }
    await serverWorker.terminate();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
JS
