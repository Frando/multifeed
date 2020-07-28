# @frando/corestore-multifeed

> Fork of [https://github.com/kappa-db/multifeed](multifeed)

Multifeed lets you group together a local set of hypercores with
a remote set of hypercores under a single shared identifier or key.

It solves the problem of [Hypercore](https://github.com/mafintosh/hypercore)
only allowing one writer per hypercore by making it easy to manage and sync
a collection of hypercores -- by a variety of authors -- across peers.

Replication works by extending the regular hypercore exchange mechanism to
include a meta-exchange, where peers share information about the feeds they
have locally, and choose which of the remote feeds they'd like to download in
exchange. Right now, the replication mechanism defaults to sharing all local
feeds and downloading all remote feeds.

This fork of multifeed removes storage of hypercores from multifeed,
instead delegating / passing this on to [corestore](https://github.com/andrewosh/corestore).
Multifeed now caches each feed using its given public key
to ensure the correct feeds are shared on replication with remote peers.
Feeds keys are persisted in an additional local-only hypercore
(it is not shared during replication), so that a multifeed instance can be easily recomposed
from our corestore, which holds no knowledge of which core belongs to which multifeed.

Using [corestore-swarm-networking](https://github.com/andrewosh/corestore-swarm-networking)
and the new multifeed networker, we can apply granular replication logic for
several multifeeds across a single instance of the [Hyperswarm DHT](https://github.com/hyperswarm/hyperswarm).

## Usage

### Simple

```
var Multifeed = require('multifeed')
var ram = require('random-access-memory')

var multi = new Multifeed('./db', { valueEncoding: 'json' })

// a multifeed starts off empty
console.log(multi.feeds().length)             // => 0

// create as many writeable feeds as you want; returns hypercores
multi.writer('local', function (err, w) {
  console.log(w.key, w.writeable, w.readable)   // => Buffer <0x..> true true
  console.log(multi.feeds().length)             // => 1

  // write data to any writeable feed, just like with hypercore
  w.append('foo', function () {
    var m2 = multifeed(hypercore, ram, { valueEncoding: 'json' })
    m2.writer('local', function (err, w2) {
      w2.append('bar', function () {
        replicate(multi, m2, function () {
          console.log(m2.feeds().length)        // => 2
          m2.feeds()[1].get(0, function (_, data) {
            console.log(data)                   // => foo
          })
          multi.feeds()[1].get(0, function (_, data) {
            console.log(data)                   // => bar
          })
        })
      })
    })
  })
})

function replicate (a, b, cb) {
  let pending = 2

  var r = a.replicate()

  r.pipe(b.replicate()).pipe(r)
    .once('error', cb)
    .once('remote-feeds', done)
    .once('remote-feeds', done)

  function done () {
    if (!--pending) return cb()
  }
}
```

### Complex

```
const Corestore = require('corestore')
const SwarmNetworker = require('corestore-swarm-networking')
const Multifeed = require('multifeed')
const Networker = require('multifeed/networker')
const crypto = require('hypercore-crypto')

// create a new corestore for our hypercores
const corestore = new Corestore(ram)

// initialize hyperswarm using corestore-swarm-networker and configure for multifeed
const network = new Networker(new SwarmNetworker(corestore))

// create two separate instances of multifeed for use across a single network swarm
const multi1 = new Multifeed(corestore, { key: crypto.randomBytes(32) })
const multi2 = new Multifeed(corestore, { key: crypto.randomBytes(32) })

// start replicating
network.swarm(multi1)
network.swarm(multi2)

// create a third instance which shares a key with the first
const multi3 = new Multifeed(corestore, { key: multi1.key })

// multi1 and multi3 will now replicate feeds
network.swarm(multi3)
```

For more information on how to implement replication across multiple multifeeds, see [test/new-basic.js](test/new-basic.js) for an example.

The main export (`new Multifeed(storage, opts)`) is API compatible with the original Multifeed as documented below. `storage` can also be a corestore (otherwise one is created with `storage`). Then connect it to a corestore swarm networker to apply multifeed replication.

## API

```js
var multifeed = require('multifeed')
```

### var multi = multifeed(storage[, opts])

Pass in a [random-access-storage](https://github.com/random-access-storage/random-access-storage) backend, and options. Included `opts` are passed into new hypercores created, and are the same as [hypercore](https://github.com/mafintosh/hypercore#var-feed--hypercorestorage-key-options)'s.

Valid `opts` include:
- `opts.encryptionKey` (string): optional encryption key to use during replication. If not provided, a default insecure key will be used.
- `opts.hypercore`: constructor of a hypercore implementation. `hypercore@8.x.x` is used from npm if not provided.

### multi.writer([name], [options], cb)

If no `name` is given, a new local writeable feed is created and returned via
`cb`.

If `name` is given and was created in the past on this local machine, it is
returned. Otherwise it is created. This is useful for managing multiple local
feeds, e.g.

```js
var main = multi.writer('main')        // created if doesn't exist
var content = multi.writer('content')  // created if doesn't exist

main === multi.writer('main')          // => true
```

`options` is an optional object which may contain: 
- `options.keypair` - an object with a custom keypair for the new writer.  This should have properties `keypair.publicKey` and `keypair.secretKey`, both of which should be buffers.

### var feeds = multi.feeds()

An array of all hypercores in the multifeed. Check a feed's `key` to
find the one you want, or check its `writable` / `readable` properties.

Only populated once `multi.ready(fn)` is fired.

### var feed = multi.feed(key)

Fetch a feed by its key `key` (a `Buffer` or hex string).

### var stream = multi.replicate(isInitiator, [opts])

Create an encrypted duplex stream for replication.

Ensure that `isInitiator` to `true` to one side, and `false` on the other. This is necessary for setting up the encryption mechanism.

Works just like hypercore, except *all* local hypercores are exchanged between
replication endpoints.

### stream.on('remote-feeds', function () { ... })

Emitted when a new batch (1 or more) of remote feeds have begun to replicate with this multifeed instance.

This is useful for knowing when `multi.feeds()` contains the full set of feeds from the remote side.

### multi.on('feed', function (feed, name) { ... })

Emitted whenever a new feed is added, whether locally or remotely.

## multi.close(cb)

Close all file resources being held by the multifeed instance. `cb` is called once this is complete.

## multi.closed

`true` if `close()` was run successfully, falsey otherwise.

# Errors

The duplex stream returned by `.replicate()` can emit, in addition to regular
stream errors, two fatal errors specific to multifeed:

- `ERR_VERSION_MISMATCH`
  - `err.code = 'ERR_VERSION_MISMATCH'`
  - `err.usVersion = 'X.Y.Z'` (semver)
  - `err.themVersion = 'A.B.C'` (semver)

- `ERR_CLIENT_MISMATCH`
  - `err.code = 'ERR_CLIENT_MISMATCH'`
  - `err.usClient = 'MULTIFEED'`
  - `err.themClient = '???'`

## Install

With [npm](https://npmjs.org/) installed, run

```
$ npm install @frando/corestore-multifeed
```

## See Also

- [multifeed (original)](https://github.com/kappa-db/multifeed)
- [multifeed-index](https://github.com/kappa-db/multifeed-index)
- [hypercore](https://github.com/mafintosh/hypercore)
- [kappa-core](https://github.com/kappa-db/kappa-core)

## License

ISC
