# amalgame-database-nosql-redis

Redis (RESP2) client for [Amalgame](https://github.com/amalgame-lang/Amalgame).
Pure-protocol implementation over raw TCP — no vendored client lib, no
`-lhiredis` at link time. Works against any RESP2-speaking server: Redis,
KeyDB, Dragonfly, Valkey.

## Install

```bash
amc add github.com/amalgame-lang/amalgame-database-nosql-redis@v0.1.0
```

Requires **amc 0.5.0+**.

## Surface

```amalgame
import Amalgame.Database.NoSQL.Redis

let r = Redis.Open("127.0.0.1", 6379)
if (!Redis.IsOpen(r)) {
    Console.WriteLine("connect failed: " + Redis.LastError(r))
    return
}

Redis.Set(r, "hello", "world")
let v: string = Redis.Get(r, "hello")     // "world"

let n: int = Redis.Incr(r, "counter")     // 1
Redis.Expire(r, "counter", 60)

Redis.Del(r, "hello")
Redis.Close(r)
```

### v0.1.0 method surface

| Method | Returns | Notes |
|---|---|---|
| `Redis.Open(host, port)` | `AmalgameRedis*` | TCP connect. Check `IsOpen` for success. |
| `Redis.Close(r)` | `void` | Idempotent |
| `Redis.IsOpen(r)` | `bool` | Connection alive? |
| `Redis.LastError(r)` | `string` | Empty on success |
| `Redis.Ping(r)` | `bool` | PINGREQ → PINGRESP |
| `Redis.Set(r, key, value)` | `bool` | Plain SET, no NX/EX flags |
| `Redis.Get(r, key)` | `string` | "" on missing key (use `Exists` to disambiguate) |
| `Redis.Del(r, key)` | `int` | Count of keys removed |
| `Redis.Exists(r, key)` | `bool` | — |
| `Redis.Incr(r, key)` | `int` | Atomic INCR; creates key if absent |
| `Redis.Decr(r, key)` | `int` | Mirror of Incr |
| `Redis.Expire(r, key, sec)` | `bool` | TTL in seconds |

### v0.3.0 additions — Pub/Sub + Pipelining

**Pub/Sub** — `Subscribe` moves the handle into subscriber mode; the connection no longer accepts ordinary commands until every channel has been unsubscribed. Open a second handle for publishing in real apps.

| Method | Returns | Notes |
|---|---|---|
| `Redis.Publish(r, channel, msg)` | `int` | Subscriber count, -1 on error |
| `Redis.Subscribe(r, channel)` | `bool` | Enters subscriber mode |
| `Redis.Unsubscribe(r, channel)` | `bool` | Leaves subscriber mode when 0 channels remain |
| `Redis.WaitMessage(r, timeoutMs)` | `bool` | True when a MSG arrived; sets last channel + last message |
| `Redis.LastChannel(r)` | `string` | Channel from most recent WaitMessage hit |
| `Redis.LastMessage(r)` | `string` | Payload from most recent WaitMessage hit |

**Pipelining** — queue many commands client-side, flush in one write, read every reply back in order. Halves round-trip cost on chains of small commands.

| Method | Returns | Notes |
|---|---|---|
| `Redis.PipelineBegin(r)` | `void` | Reset the queue, enter pipeline mode |
| `Redis.PipelineSet(r, k, v)` | `void` | Queue SET |
| `Redis.PipelineGet(r, k)` | `void` | Queue GET |
| `Redis.PipelineIncr(r, k)` | `void` | Queue INCR |
| `Redis.PipelineDecr(r, k)` | `void` | Queue DECR |
| `Redis.PipelineDel(r, k)` | `void` | Queue DEL |
| `Redis.PipelineExpire(r, k, sec)` | `void` | Queue EXPIRE |
| `Redis.PipelineExec(r)` | `int` | Flush + read; returns reply count |
| `Redis.PipelineResponseAt(r, idx)` | `string` | Reply at queue position idx |

```amalgame
let r = Redis.Open("127.0.0.1", 6379)

// Pipeline 3 SETs + 1 INCR in one round-trip.
Redis.PipelineBegin(r)
Redis.PipelineSet(r, "user:1", "alice")
Redis.PipelineSet(r, "user:2", "bob")
Redis.PipelineSet(r, "user:3", "carol")
Redis.PipelineIncr(r, "user:count")
let n = Redis.PipelineExec(r)
Console.WriteLine("user count: " + Redis.PipelineResponseAt(r, 3))  // "3"

// Pub/Sub on a fresh connection.
let sub = Redis.Open("127.0.0.1", 6379)
Redis.Subscribe(sub, "events")
Redis.Publish(r, "events", "hello")
if (Redis.WaitMessage(sub, 1000)) {
    Console.WriteLine(Redis.LastChannel(sub) + " → " + Redis.LastMessage(sub))
}
Redis.Unsubscribe(sub, "events")
Redis.Close(sub); Redis.Close(r)
```

## Deferred to v0.4+

AUTH, SELECT db, MULTI/EXEC transactions, SCAN array replies, TLS,
binary-safe values with embedded NULs, PSUBSCRIBE pattern matching,
hash / list / set / sorted-set surface.

## Threading

`AmalgameRedis*` is single-owner. Concurrent calls on the same
handle from different threads are undefined; wrap in a mutex if
you need it. Different handles are safe to use concurrently.

## Building

```bash
amc -o myapp src/main.am
# Generated .c references the runtime header from the package
# cache. amc emits the right `-I` flag automatically.
```

## Tests

```bash
./tests/run_tests.sh /path/to/amc
```

The runner probes `127.0.0.1:6379` for a running server; if none
is reachable every test SKIPs cleanly. Start a server locally
with `docker run --rm -p 6379:6379 redis:7` or `sudo apt install
redis-server && redis-server &`.

## Licence

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE.md`](NOTICE.md).
