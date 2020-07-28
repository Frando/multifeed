const Corestore = require('corestore')
const ram = require('random-access-memory')
const dht = require('@hyperswarm/dht')
const Networker = require('corestore-networker')
const BOOTSTRAP_PORT = 3100
var bootstrap = null

module.exports = { create, cleanup, BOOTSTRAP_PORT }

async function create (opts = {}) {
  if (!bootstrap) {
    bootstrap = dht({
      bootstrap: false
    })
    bootstrap.listen(BOOTSTRAP_PORT)
    await new Promise(resolve => {
      return bootstrap.once('listening', resolve)
    })
  }
  const store = new Corestore(ram)
  await store.ready()
  const networker = new Networker(store, {
    ...opts,
    bootstrap: `localhost:${BOOTSTRAP_PORT}`
  })
  // logEvents(networker, 'networker')
  return { store, networker }
}

async function cleanup (networkers) {
  for (let networker of networkers) {
    await networker.close()
  }
  if (bootstrap) {
    await bootstrap.destroy()
    bootstrap = null
  }
}

function logEvents (emitter, name) {
  const emit = emitter.emit.bind(emitter)
  emitter.emit = function (event, ...args) {
    console.log(name, event)
    if (event === 'replication-error') console.log(args)
    emit(event, ...args)
  }
}
