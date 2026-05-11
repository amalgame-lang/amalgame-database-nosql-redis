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

## Deferred to v2

AUTH, SELECT db, pipelining, pub/sub, MULTI/EXEC transactions, SCAN
array replies, TLS, binary-safe values with embedded NULs.

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
