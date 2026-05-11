# NOTICE — amalgame-database-nosql-redis

## Authorship

Copyright 2026 Bastien Mouget. The Amalgame binding code in this
repository is original work — see `runtime/Amalgame_Database_Redis.h`.

This package is part of the Amalgame ecosystem
([github.com/amalgame-lang/Amalgame](https://github.com/amalgame-lang/Amalgame)).
External contributions are paused at the ecosystem level; see the
main repo's `CONTRIBUTING.md` for the policy.

AI tools (Anthropic Claude) were used during development. Per
the project's authorship policy, AI is treated as a tool, not a
co-author at law.

## Licence

Apache License 2.0. See `LICENSE` for the full text.

## Third-party content

**None vendored.** This package implements the RESP2 wire
protocol directly in ~340 LoC of C against the cross-platform
socket layer that amc already ships. There is no
`libhiredis` / `libredis++` / `libredis-rs` dependency, no
embedded fork of upstream Redis client code. The wire format
itself ([RESP2 specification](https://redis.io/docs/latest/develop/reference/protocol-spec/))
is published by Redis Ltd. but is not copyrightable as a protocol
specification — only the reference implementation source code is.

## Trademarks

"Redis" is a registered trademark of Redis Ltd. This repository
uses the name solely to identify the protocol being implemented
and the server families that speak it. No trademark claim is
asserted. The same client works against any RESP2-compatible
server (KeyDB, Dragonfly, Valkey) without modification.
