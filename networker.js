const crypto = require('hypercore-crypto')
const Multiplexer = require('./mux')
const debug = require('debug')('multifeed')
const { EventEmitter } = require('events')

class MuxerTopic extends EventEmitter {
  constructor (corestore, rootKey, opts = {}) {
    super()
    this._id = crypto.randomBytes(2).toString('hex')
    this.corestore = corestore
    this.rootKey = rootKey
    this._feeds = new Map()
    this._streams = new Map()
    this._opts = opts
    this.getFeed = opts.getFeed || this._getFeed.bind(this)
  }

  // Add a stream to the multiplexer.
  //
  // This will open the multifeed muxer extensions on a new channel
  // that is opened for this muxer's rootKey.
  addStream (stream, opts) {
    const self = this

    // TODO: There seem to be bugs with non-live mode. Investigate.
    opts = { ...this._opts, ...opts, stream, live: true }

    const mux = new Multiplexer(null, this.rootKey, opts)

    mux.on('manifest', onmanifest)
    mux.on('replicate', onreplicate)
    mux.ready(onready)

    stream.once('end', cleanup)
    stream.once('error', cleanup)
    this._streams.set(stream, { mux, cleanup })

    function onready () {
      const keys = Array.from(self._feeds.keys())
      mux.offerFeeds(keys)
    }

    function onmanifest (manifest) {
      mux.requestFeeds(manifest.keys)
    }

    function onreplicate (keys, repl) {
      let pending = keys.length

      for (const key of keys) {
        if (self._feeds.has(key)) {
          done()
          continue
        }

        self.getFeed(key, (err, feed) => {
          if (err) return done(err)
          self.addFeed(feed)
          self.emit('feed', feed)
          done()
        })
      }

      function done () {
        if (!--pending) {
          const feeds = keys.map(key => self._feeds.get(key))
          repl(feeds)
        }
      }
    }

    function cleanup (_err) {
      mux.removeListener('manifest', onmanifest)
      mux.removeListener('replicate', onreplicate)
      self._streams.delete(stream)
    }
  }

  removeStream (stream) {
    if (!this._streams.has(stream)) return
    const { cleanup } = this._streams.get(stream)
    cleanup()
  }

  feeds () {
    return Array.from(this._feeds.values())
  }

  _getFeed (key, cb) {
    var feed = this.corestore.get({ key })
    if (!feed) return cb(new Error('no feed matching that key'))
    cb(null, feed)
  }

  addFeed (feed) {
    const hkey = feed.key.toString('hex')
    this._feeds.set(hkey, feed)
    for (const { mux } of this._streams.values()) {
      mux.ready(() => {
        if (mux.knownFeeds().indexOf(hkey) === -1) {
          debug('Forwarding new feed to existing peer:', hkey)
          mux.offerFeeds([hkey])
        }
      })
    }
  }
}

module.exports = class MultifeedNetworker {
  constructor (networker) {
    this.networker = networker
    this.corestore = networker.corestore
    this.muxers = new Map()
    this.streamsByKey = new Map()

    this._joinListener = this._onjoin.bind(this)
    this._leaveListener = this._onleave.bind(this)
    this.networker.on('handshake', this._joinListener)
    this.networker.on('stream-closed', this._leaveListener)
  }

  _onjoin (stream, info) {
    const remoteKey = stream.remotePublicKey
    const keyString = remoteKey.toString('hex')
    this.streamsByKey.set(keyString, { stream, info })
    // Add the incoming stream to all multiplexers.
    for (const mux of this.muxers.values()) {
      mux.addStream(stream)
    }
  }

  _onleave (stream, info, finishedHandshake) {
    if (!finishedHandshake || (info && info.duplicate)) return
    for (const mux of this.muxers.values()) {
      mux.removeStream(stream)
    }
  }

  swarm (multifeed, opts = {}) {
    multifeed.ready(() => {
      this.join(multifeed.key, { live: true, mux: multifeed._muxer, ...opts })
    })
  }

  join (rootKey, opts = {}) {
    if (!Buffer.isBuffer(rootKey)) rootKey = Buffer.from(rootKey, 'hex')
    const hkey = rootKey.toString('hex')
    if (this.muxers.has(hkey)) return this.muxers.get(hkey)
    const mux = opts.mux || new MuxerTopic(this.corestore, rootKey, opts)
    const discoveryKey = crypto.discoveryKey(rootKey)
    // Join the swarm.
    this.networker.configure(discoveryKey, { announce: true, lookup: true })
    this.muxers.set(hkey, mux)
    // Add all existing streams to the multiplexer.
    for (const { stream } of this.streamsByKey.values()) {
      mux.addStream(stream)
    }
    return mux
  }

  leave (rootKey) {
    if (!Buffer.isBuffer(rootKey)) rootKey = Buffer.from(rootKey, 'hex')
    const hkey = rootKey.toString('hex')
    var mux = this.muxers.get(hkey)
    if (!mux) return false // throw??
    const discoveryKey = crypto.discoveryKey(rootKey)
    this.networker.configure(discoveryKey, { announce: false, lookup: false })
    this.muxers.delete(hkey)
    // remove and close any existing streams from this mux instance
    for (const { stream } of this.streamsByKey.values()) {
      mux.removeStream(stream)
    }
    return true
  }
}

module.exports.MuxerTopic = MuxerTopic
