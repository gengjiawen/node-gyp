'use strict'

// EXPERIMENT (experiment/h2-ab): from this machine, fetch the headers tarball
// and stop reading after the first chunk for PAUSES seconds, over HTTP/2 and
// HTTP/1.1, to see whether nodejs.org resets stalled streams.

const { Agent, RetryAgent, fetch } = require('undici')

const url = `https://nodejs.org/dist/${process.version}/node-${process.version}-headers.tar.gz`
const pauses = (process.env.PAUSES || '30,60,120,180').split(',').map(Number)

async function probe (h2, pause) {
  const t0 = Date.now()
  const res = await fetch(url, { dispatcher: new RetryAgent(new Agent({ allowH2: h2 }), { maxRetries: 3 }) })
  const pop = `${res.headers.get('cf-cache-status')} ${(res.headers.get('cf-ray') || '').split('-')[1]}`
  let bytes = 0
  let first = true
  try {
    for await (const chunk of res.body) {
      bytes += chunk.length
      if (first) {
        first = false
        await new Promise((resolve) => setTimeout(resolve, pause * 1000))
      }
    }
    return { h2, pause, pop, ok: true, bytes, s: Math.round((Date.now() - t0) / 1000) }
  } catch (err) {
    return { h2, pause, pop, ok: false, bytes, s: Math.round((Date.now() - t0) / 1000), error: `${err.message} <- ${err.cause?.message}` }
  }
}

Promise.all(pauses.flatMap((p) => [probe(true, p), probe(true, p), probe(false, p)])).then((results) => {
  for (const r of results) console.log('PROBE ' + JSON.stringify(r))
})
