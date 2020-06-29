const test = require('tape')
const hcrypto = require('hypercore-crypto')
const MultifeedNetworker = require('../networker')
const Multifeed = require('..')
const { create, cleanup } = require('./lib/networker')

test.only('minimal swarm example', async function (t) {
  const { store: store1, networker: networker1 } = await create()
  const { store: store2, networker: networker2 } = await create()

  const swarm1 = new MultifeedNetworker(networker1)
  const swarm2 = new MultifeedNetworker(networker2)

  const rootKey = hcrypto.keyPair().publicKey

  const multi1 = new Multifeed(store1, { rootKey })
  swarm1.swarm(multi1)
  await ready(multi1)

  const multi2 = new Multifeed(store2, { rootKey })
  swarm2.swarm(multi2)
  await ready(multi2)

  const writer1 = await writer(multi1, 'foo')
  const writer2 = await writer(multi2, 'bar')
  await append(writer1, 'first')
  await append(writer2, 'second')

  await timeout(500)

  t.deepEqual(toKeys(multi1.feeds()), toKeys([writer1, writer2]))
  t.deepEqual(toKeys(multi2.feeds()), toKeys([writer1, writer2]))
  t.deepEqual(
    await get(await writer(multi1, '1'), 0),
    Buffer.from('second')
  )
  t.deepEqual(
    await get(await writer(multi2, '1'), 0),
    Buffer.from('first')
  )
  await cleanup([networker1, networker2])
})

function toKeys (feeds) {
  return feeds.map(f => f.key.toString('hex')).sort()
}

function ready (resource) {
  return new Promise((resolve, reject) => {
    resource.ready(err => {
      if (err) return reject(err)
      resolve()
    })
  })
}

function writer (multifeed, name) {
  return new Promise((resolve, reject) => {
    multifeed.writer(name, (err, feed) => {
      if (err) return reject(err)
      resolve(feed)
    })
  })
}

function append (core, data) {
  return new Promise((resolve, reject) => {
    core.append(data, err => {
      if (err) return reject(err)
      return resolve()
    })
  })
}
function get (core, idx, opts = {}) {
  return new Promise((resolve, reject) => {
    core.get(idx, opts, (err, data) => {
      if (err) return reject(err)
      return resolve(data)
    })
  })
}

function timeout (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}
