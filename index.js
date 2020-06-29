const Corestore = require('corestore')
const hcrypto = require('hypercore-crypto')
const Protocol = require('hypercore-protocol')
const Nanoresource = require('nanoresource/emitter')
const collect = require('stream-collector')
const debug = require('debug')('multifeed')
const raf = require('random-access-file')
const through = require('through2')

const { MuxerTopic } = require('./networker')

// Default key to bootstrap replication and namespace the corestore
// It is not adviced to use this for real purposes. If no root key is
// passed in, this key will be used for opening a protocol channel
// and as the namespace to store the list of feeds that are part of this
// multifeed (if using the default persist handlers).
const DEFAULT_ROOT_KEY = Buffer.from('bee80ff3a4ee5e727dc44197cb9d25bf8f19d50b0f3ad2984cfe5b7d14e75de7', 'hex')

const MULTIFEED_NAMESPACE_PREFIX = '@multifeed:'
const FEED_NAMESPACE_PREFIX = '@multifeed:feed:'
const PERSIST_NAMESPACE = '@multifeed:persist'

class Multifeed extends Nanoresource {
  constructor (storage, opts) {
    super()
    this._opts = opts
    this._rootKey = opts.rootKey || opts.encryptionKey || opts.key
    if (this._rootKey && !Buffer.isBuffer(this._rootKey)) {
      this._rootKey = Buffer.from(this._rootKey, 'hex')
    }
    if (!this._rootKey) {
      debug('WARNING: Using insecure default root key')
      this._rootKey = DEFAULT_ROOT_KEY
    }
    this._corestore = defaultCorestore(storage, opts)
      .namespace(MULTIFEED_NAMESPACE_PREFIX + this._rootKey.toString('hex'))

    this._handlers = opts.handlers || defaultPersistHandlers(this._corestore)
    this._feedsByKey = new Map()
    this._feedsByName = new Map()
    this.ready = this.open.bind(this)
  }

  get key () {
    return this._rootKey
  }

  get discoveryKey () {
    if (!this._discoveryKey) this._discoveryKey = hcrypto.discoveryKey(this._rootKey)
    return this._discoveryKey
  }

  _open (cb) {
    this._corestore.ready(err => {
      if (err) return cb(err)
      this._muxer = new MuxerTopic(this._rootKey, this._corestore)
      this._muxer.on('feed', feed => {
        this._addFeed(feed, null, true)
      })
      this._fetchFeeds(cb)
    })
  }

  _close (cb) {
    const self = this
    let pending = 1
    if (this._handlers.close) ++pending && this._handlers.close(onclose)
    this._corestore.close(onclose)
    function onclose () {
      if (--pending !== 0) return
      self._feedsByKey = new Map()
      self._feedsByName = new Map()
      self._rootKey = null
      cb()
    }
  }

  _addFeed (feed, name, save = false) {
    if (this._feedsByKey.has(feed.key.toString('hex'))) return
    if (!name) name = String(this._feedsByKey.size)
    if (save) this._storeFeed(feed, name)
    this._feedsByName.set(name, feed)
    this._feedsByKey.set(feed.key.toString('hex'), feed)
    this._muxer.addFeed(feed)
    this.emit('feed', feed, name)
  }

  _storeFeed (feed, name) {
    const info = { key: feed.key.toString('hex'), name }
    this._handlers.storeFeed(info, err => {
      if (err) this.emit('error', err)
    })
  }

  _fetchFeeds (cb) {
    this._handlers.fetchFeeds((err, infos) => {
      if (err) return cb(err)
      for (const info of infos) {
        const feed = this._corestore.get({ key: info.key })
        this._addFeed(feed, info.name, false)
      }
      cb()
    })
  }

  writer (name, opts, cb) {
    if (!this.opened) return this.ready(() => this.writer(name, opts, cb))
    if (typeof name === 'function' && !cb) {
      cb = name
      name = undefined
      opts = {}
    }
    if (typeof opts === 'function' && !cb) {
      cb = opts
      opts = {}
    }
    if (this._feedsByName.has(name)) return cb(null, this._feedsByName.get(name))
    // TODO: Only support keyPair
    if (opts.keypair) opts.keyPair = opts.keypair
    const namespace = FEED_NAMESPACE_PREFIX + name
    const feed = this._corestore.namespace(namespace).default(opts)
    this._addFeed(feed, name, true)
    feed.ready(() => {
      cb(null, feed)
    })
  }

  feeds () {
    return Array.from(this._feedsByKey.values())
  }

  feed (key) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    if (typeof key === 'string') return this._feedsByKey.get(key)
    else return null
  }

  replicate (isInitiator, opts = {}) {
    if (!this.opened) {
      return errorStream(new Error('tried to use "replicate" before multifeed is ready'))
    }
    const stream = opts.stream || new Protocol(isInitiator, opts)
    this._muxer.addStream(stream, opts)
    return stream
  }
}

function errorStream (err) {
  var tmp = through()
  process.nextTick(function () {
    tmp.emit('error', err)
  })
  return tmp
}

function isCorestore (storage) {
  return storage.default && storage.get && storage.replicate && storage.close
}

function defaultPersistHandlers (corestore) {
  const namespacedStore = corestore.namespace(PERSIST_NAMESPACE)
  let feed
  return {
    fetchFeeds (cb) {
      feed = namespacedStore.default({ valueEncoding: 'json' })
      feed.ready(err => {
        if (err) return cb(err)
        collect(feed.createReadStream(), cb)
      })
    },

    storeFeed (info, cb) {
      feed.ready(err => {
        if (err) return cb(err)
        feed.append(info, cb)
      })
    },

    close (cb) {
      namespacedStore.close(cb)
    }
  }
}

function defaultCorestore (storage, opts) {
  if (isCorestore(storage)) return storage
  if (typeof storage === 'function') {
    var factory = path => storage(path)
  } else if (typeof storage === 'string') {
    factory = path => raf(storage + '/' + path)
  }
  return new Corestore(factory, opts)
}

module.exports = function (...args) { return new Multifeed(...args) }
module.exports.defaultCorestore = defaultCorestore
