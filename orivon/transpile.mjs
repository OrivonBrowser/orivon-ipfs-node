// Transpiles a wasm32-wasip2 command component into the JavaScript + core
// modules Orivon's WASI host instantiates. Options are the ones the host's
// own transpile command names (jco transpile --instantiation async
// --async-mode jspi --no-nodejs-compat, with the host's async import list),
// driven through jco-transpile's library API because jco is not a
// dependency of this repository: ORIVON_JCO_DIR is an npm prefix with
// @bytecodealliance/jco-transpile@0.15.0 installed.
//
// usage: node transpile.mjs <component.wasm> <out-dir> [name]

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

const JCO_DIR = process.env.ORIVON_JCO_DIR
if (!JCO_DIR) {
  console.error('transpile.mjs: set ORIVON_JCO_DIR')
  process.exit(1)
}

const ASYNC_IMPORTS = [
  'wasi:io/poll#poll',
  'wasi:io/poll#[method]pollable.block',
  ...['blocking-read', 'blocking-skip'].map((name) => `wasi:io/streams#[method]input-stream.${name}`),
  ...['blocking-write-and-flush', 'blocking-flush', 'blocking-write-zeroes-and-flush', 'blocking-splice'].map((name) => `wasi:io/streams#[method]output-stream.${name}`),
  'wasi:sockets/tcp#[method]tcp-socket.finish-listen',
  'wasi:sockets/udp#[method]udp-socket.finish-bind',
  ...[
    'sync-data', 'sync', 'set-size', 'read', 'write', 'read-directory', 'create-directory-at', 'stat', 'stat-at',
    'readlink-at', 'open-at', 'remove-directory-at', 'rename-at', 'unlink-file-at', 'metadata-hash', 'metadata-hash-at'
  ].map((name) => `wasi:filesystem/types#[method]descriptor.${name}`)
]
const ASYNC_EXPORTS = ['wasi:cli/run#run']

const [componentPath, outDir, name = 'ipfs-node'] = process.argv.slice(2)
if (!componentPath || !outDir) {
  console.error('usage: node transpile.mjs <component.wasm> <out-dir> [name]')
  process.exit(1)
}

const bindgen = await import(pathToFileURL(join(JCO_DIR, 'node_modules/@bytecodealliance/jco-transpile/vendor/js-component-bindgen-component.js')).href)
await bindgen.$init

const { files } = bindgen.generate(readFileSync(componentPath), {
  name,
  instantiation: { tag: 'async' },
  asyncMode: { tag: 'jspi', val: { imports: ASYNC_IMPORTS, exports: ASYNC_EXPORTS } },
  noNodejsCompat: true,
  noTypescript: true,
  map: []
})

mkdirSync(outDir, { recursive: true })
for (const [file, bytes] of files) {
  writeFileSync(join(outDir, file), bytes)
  console.log(`${file}\t${bytes.length}`)
}
