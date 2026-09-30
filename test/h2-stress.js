'use strict'

// EXPERIMENT (experiment/h2-ab): repeat the `install > parallel` test from
// test/test-install.js in a time-boxed loop and record, per download, the
// negotiated protocol, the longest consumer stall, and any error.
//
//   NODE_GYP_TEST_ALLOW_H2=0|1  CONCURRENCY=5  BUDGET_MIN=20  OUT=result.json
//   STALL_INJECT_MS=n  self-test: pause the first tarball download once for n ms

const { mkdtemp, rm, writeFile } = require('fs/promises')
const { PassThrough, pipeline } = require('stream')
const diagnostics = require('diagnostics_channel')
const os = require('os')
const path = require('path')

const dl = require('../lib/download')

const H2 = process.env.NODE_GYP_TEST_ALLOW_H2 !== '0'
const CONCURRENCY = Number(process.env.CONCURRENCY || 5)
const BUDGET = Number(process.env.BUDGET_MIN || 20) * 60 * 1000
const OUT = process.env.OUT || 'result.json'
let stallInject = Number(process.env.STALL_INJECT_MS || 0)

const alpn = {}
diagnostics.channel('undici:client:connected').subscribe(({ socket }) => {
  const proto = socket.alpnProtocol || 'none'
  alpn[proto] = (alpn[proto] || 0) + 1
})

let round = 0
const downloads = []

// Must be patched before lib/install.js is required, it destructures download.
const origDownload = dl.download
dl.download = async (gyp, url) => {
  const rec = { round, file: path.basename(url), ms: 0, bytes: 0, maxGapMs: 0, gapBeforeErrorMs: null, error: null }
  downloads.push(rec)
  const start = Date.now()
  let res
  try {
    res = await origDownload(gyp, url)
  } catch (err) {
    rec.error = `${err.code || ''} ${err.message}`.trim()
    throw err
  }
  if (url.endsWith('.txt')) return res // SHASUMS256.txt is read via res.text()

  // Data only moves from res.body into `out` when the consumer (tar / file
  // write) has drained it, so gaps between chunks are consumer stalls.
  let last = Date.now()
  const out = new PassThrough()
  res.body.on('data', (chunk) => {
    const now = Date.now()
    rec.maxGapMs = Math.max(rec.maxGapMs, now - last)
    last = now
    rec.bytes += chunk.length
    if (stallInject && url.endsWith('.tar.gz')) {
      res.body.pause()
      setTimeout(() => res.body.resume(), stallInject)
      stallInject = 0
    }
  })
  pipeline(res.body, out, (err) => {
    rec.ms = Date.now() - start
    if (err) {
      rec.gapBeforeErrorMs = Date.now() - last
      const cause = err.cause ? ` <- ${err.cause.code || ''} ${err.cause.message}` : ''
      rec.error = `${err.code || ''} ${err.message}${cause}`.trim()
    }
  })
  return { ...res, body: out }
}

const gyp = require('../lib/node-gyp')
const install = require('../lib/install')

async function main () {
  const rounds = []
  const t0 = Date.now()
  while (Date.now() - t0 < BUDGET) {
    round++
    const prog = gyp()
    prog.parseArgv([])
    prog.devDir = await mkdtemp(path.join(os.tmpdir(), 'node-gyp-h2ab-'))
    prog.opts.ensure = round % 2 === 1
    const r0 = Date.now()
    const results = await Promise.allSettled(Array.from({ length: CONCURRENCY }, () => install(prog, [])))
    const failed = results.filter((r) => r.status === 'rejected')
    rounds.push({
      round,
      ensure: prog.opts.ensure,
      ms: Date.now() - r0,
      installs: results.length,
      failed: failed.length,
      errors: failed.map((r) => `${r.reason.code || ''} ${r.reason.message}${r.reason.cause ? ' <- ' + r.reason.cause.message : ''}`.trim())
    })
    console.log(`round ${round} ensure=${prog.opts.ensure} ${((Date.now() - r0) / 1000).toFixed(0)}s failed=${failed.length}/${results.length}`)
    await rm(prog.devDir, { recursive: true, force: true, maxRetries: 3, retryDelay: 1000 })
  }

  const summary = {
    platform: `${process.platform}-${process.arch}`,
    node: process.version,
    h2: H2,
    concurrency: CONCURRENCY,
    alpn,
    rounds: rounds.length,
    failedRounds: rounds.filter((r) => r.failed).length,
    installs: rounds.reduce((a, r) => a + r.installs, 0),
    failedInstalls: rounds.reduce((a, r) => a + r.failed, 0)
  }
  console.log('SUMMARY ' + JSON.stringify(summary))
  const result = JSON.stringify({ summary, rounds, downloads })
  console.log('RESULT ' + result)
  await writeFile(OUT, result)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
