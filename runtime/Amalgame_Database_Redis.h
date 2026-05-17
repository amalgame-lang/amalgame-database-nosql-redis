/*
 * Amalgame Standard Library — Amalgame.Database.NoSQL.Redis
 * Copyright (c) 2026 Bastien MOUGET
 * https://github.com/amalgame-lang/Amalgame
 *
 * Redis client speaking the RESP (Redis Serialization Protocol) v2
 * directly over a BSD-socket TCP connection. No external client lib —
 * the protocol fits in ~250 lines of C and the wire format is identical
 * between Redis, KeyDB, Dragonfly, and Valkey, so this binding works
 * against all of them.
 *
 * Surface (v1):
 *   Open / Close / IsOpen / LastError      — lifecycle
 *   Ping                                   — health probe
 *   Set / Get / Del / Exists               — key/value
 *   Incr / Decr                            — atomic counters
 *   Expire                                 — TTL
 *
 * Out of scope for v1 (tracked for v2):
 *   AUTH / SELECT db, pipelining, pub/sub, MULTI/EXEC transactions,
 *   SCAN / KEYS array replies, binary-safe values with embedded NULs,
 *   TLS, connection pooling, auto-reconnect.
 *
 * Reuses the cross-platform socket layer from Amalgame_Net.h
 * (`_amnet_init_once`, `_amnet_close_socket`, the winsock2 vs BSD
 * conditional). User binaries linking against this header don't need
 * libcurl unless they also pull in `Http_*` — the curl includes in
 * Amalgame_Net.h are gated on `__has_include`.
 *
 * Threading: an `AmalgameRedis*` handle is single-owner. Concurrent
 * calls on the same handle from different threads are undefined; if
 * you need shared access, wrap the handle in a mutex on the caller
 * side. Concurrent calls on different handles are safe.
 */

#ifndef AMALGAME_DATABASE_REDIS_H
#define AMALGAME_DATABASE_REDIS_H

#include "_runtime.h"
#include "Amalgame_Collections.h"
#include "Amalgame_Net.h"   /* cross-platform sockets + _amnet_init_once */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct AmalgameRedis {
    int         fd;          /* socket fd; -1 = not connected */
    char*       last_error;  /* GC-strdup'd error message, or NULL */

    /* ── Pub/Sub state (v0.3) ────────────────────────── */
    /* Once Subscribe is called, the connection enters subscriber
     * mode — the server pushes MSG arrays unsolicited. WaitMessage
     * blocks for the next one and stashes its channel + payload
     * here so the AM side can read them via dedicated verbs. */
    char*       last_channel;
    char*       last_message;
    int         in_subscriber_mode;

    /* ── Pipeline state (v0.3) ───────────────────────── */
    /* PipelineBegin sets `in_pipeline=1` and resets the buffer;
     * subsequent Pipeline* commands append wire bytes WITHOUT
     * reading the reply. PipelineExec flushes the buffer and
     * reads exactly `pipeline_count` replies in order. */
    char*       pipeline_buf;
    size_t      pipeline_buf_n;
    size_t      pipeline_buf_cap;
    int         pipeline_count;
    int         in_pipeline;
    AmalgameList* pipeline_responses;   /* List<string>, set by Exec */
} AmalgameRedis;

/* GC-dup an error message into a stable buffer. */
static inline code_string _amredis_err_dup(const char* msg) {
    if (!msg) return NULL;
    size_t n = strlen(msg);
    char* p = (char*) code_alloc(n + 1);
    memcpy(p, msg, n + 1);
    return p;
}

/* ── Lifecycle ──────────────────────────────────────── */

/* Open a TCP connection to redis://host:port. Returns a non-NULL
 * handle even on failure — call Redis.IsOpen() to check, or
 * Redis.LastError() for the message. */
static inline AmalgameRedis* Amalgame_Database_NoSQL_Redis_Open(code_string host, i64 port) {
    _amnet_init_once();
    AmalgameRedis* r = (AmalgameRedis*) code_alloc(sizeof(AmalgameRedis));
    r->fd         = -1;
    r->last_error = NULL;
    r->last_channel        = NULL;
    r->last_message        = NULL;
    r->in_subscriber_mode  = 0;
    r->pipeline_buf        = NULL;
    r->pipeline_buf_n      = 0;
    r->pipeline_buf_cap    = 0;
    r->pipeline_count      = 0;
    r->in_pipeline         = 0;
    r->pipeline_responses  = NULL;

    if (!host || !*host) {
        r->last_error = _amredis_err_dup("host is empty");
        return r;
    }

    char portStr[16];
    snprintf(portStr, sizeof(portStr), "%lld", (long long) port);

    struct addrinfo hints = {0};
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* res = NULL;
    if (getaddrinfo(host, portStr, &hints, &res) != 0) {
        r->last_error = _amredis_err_dup("getaddrinfo failed");
        return r;
    }

    int fd = (int) socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        r->last_error = _amredis_err_dup("socket() failed");
        freeaddrinfo(res);
        return r;
    }
    if (connect(fd, res->ai_addr, (int) res->ai_addrlen) != 0) {
        r->last_error = _amredis_err_dup("connect() failed");
        _amnet_close_socket(fd);
        freeaddrinfo(res);
        return r;
    }
    freeaddrinfo(res);
    r->fd = fd;
    return r;
}

/* Close the socket. Idempotent. The wrapper struct itself is
 * GC-managed; we don't free it here. */
static inline void Amalgame_Database_NoSQL_Redis_Close(AmalgameRedis* r) {
    if (r && r->fd >= 0) {
        _amnet_close_socket(r->fd);
        r->fd = -1;
    }
}

static inline code_bool Amalgame_Database_NoSQL_Redis_IsOpen(AmalgameRedis* r) {
    return (r && r->fd >= 0) ? 1 : 0;
}

static inline code_string Amalgame_Database_NoSQL_Redis_LastError(AmalgameRedis* r) {
    if (!r) return "";
    return r->last_error ? r->last_error : "";
}

/* ── Wire format (RESP2) ────────────────────────────── */

/* send() can short-write; this loops until every byte is on the
 * wire or the socket errors. Returns 0 on success, -1 on error. */
static inline int _amredis_send_all(int fd, const char* buf, size_t n) {
    size_t off = 0;
    while (off < n) {
        ssize_t k = send(fd, buf + off, n - off, 0);
        if (k <= 0) return -1;
        off += (size_t) k;
    }
    return 0;
}

/* Encode a command as a RESP array of bulk strings.
 *   SET foo bar → *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
 *
 * Sized two-pass: first compute the total length, allocate exactly
 * that much, then fill in. Lets us send the whole command in one
 * send() and avoids fragmentation across multiple syscalls. */
static inline char* _amredis_build_cmd(int argc, const char* const* args, size_t* out_n) {
    size_t total = 0;
    char tmp[24];

    int hlen = snprintf(tmp, sizeof(tmp), "*%d\r\n", argc);
    total += (size_t) hlen;
    for (int i = 0; i < argc; i++) {
        const char* a = args[i] ? args[i] : "";
        size_t alen   = strlen(a);
        int llen      = snprintf(tmp, sizeof(tmp), "$%zu\r\n", alen);
        total += (size_t) llen + alen + 2; /* + trailing CRLF */
    }

    char* buf = (char*) code_alloc(total + 1);
    size_t pos = 0;
    int wrote = snprintf(buf + pos, total + 1 - pos, "*%d\r\n", argc);
    pos += (size_t) wrote;
    for (int i = 0; i < argc; i++) {
        const char* a = args[i] ? args[i] : "";
        size_t alen   = strlen(a);
        wrote = snprintf(buf + pos, total + 1 - pos, "$%zu\r\n", alen);
        pos += (size_t) wrote;
        memcpy(buf + pos, a, alen);
        pos += alen;
        buf[pos++] = '\r';
        buf[pos++] = '\n';
    }
    buf[pos] = '\0';
    *out_n = pos;
    return buf;
}

/* Read one RESP line (everything up to \r\n) into a GC buffer.
 * Returns NULL on EOF / socket error. Caller does NOT include the
 * leading prefix byte ('+', '-', ':', '$', '*') — that's read
 * separately by _amredis_read_reply. */
static inline char* _amredis_read_line(int fd) {
    size_t cap = 64;
    size_t n   = 0;
    char*  buf = (char*) code_alloc(cap);
    char   ch;
    while (1) {
        ssize_t k = recv(fd, &ch, 1, 0);
        if (k <= 0) return NULL;
        if (ch == '\r') {
            /* RESP frames end with CRLF; consume the LF and bail. */
            recv(fd, &ch, 1, 0);
            buf[n] = '\0';
            return buf;
        }
        if (n + 1 >= cap) {
            cap *= 2;
            char* nb = (char*) code_alloc(cap);
            memcpy(nb, buf, n);
            buf = nb;
        }
        buf[n++] = ch;
    }
}

/* Read exactly N bytes followed by \r\n. Used for bulk-string
 * payloads after we've parsed the length header. */
static inline char* _amredis_read_bulk(int fd, size_t n) {
    char* buf = (char*) code_alloc(n + 1);
    size_t off = 0;
    while (off < n) {
        ssize_t k = recv(fd, buf + off, n - off, 0);
        if (k <= 0) return NULL;
        off += (size_t) k;
    }
    buf[n] = '\0';
    /* Discard trailing CRLF — Redis always sends it. */
    char cr;
    recv(fd, &cr, 1, 0);
    recv(fd, &cr, 1, 0);
    return buf;
}

/* Parsed RESP reply. `kind` carries the prefix byte; on protocol /
 * socket error we report 'x' and the caller treats that as a
 * connection-level failure (distinct from a Redis-level '-' error). */
typedef struct {
    char        kind;     /* '+', '-', ':', '$', or 'x' on error */
    code_string str_val;  /* +, -, $ → the payload as a GC string */
    i64         int_val;  /* : → integer */
    code_bool   is_nil;   /* $ with length -1 (nil bulk string) */
} _AmRedisReply;

static inline _AmRedisReply _amredis_read_reply(int fd) {
    _AmRedisReply rep;
    rep.kind    = 'x';
    rep.str_val = "";
    rep.int_val = 0;
    rep.is_nil  = 0;

    char prefix;
    ssize_t k = recv(fd, &prefix, 1, 0);
    if (k <= 0) return rep;

    char* line = _amredis_read_line(fd);
    if (!line) return rep;

    rep.kind = prefix;
    if (prefix == '+' || prefix == '-') {
        rep.str_val = line;
    } else if (prefix == ':') {
        rep.int_val = (i64) atoll(line);
    } else if (prefix == '$') {
        long long len = atoll(line);
        if (len < 0) {
            rep.is_nil  = 1;
            rep.str_val = "";
        } else {
            char* data = _amredis_read_bulk(fd, (size_t) len);
            if (!data) { rep.kind = 'x'; rep.str_val = ""; }
            else       { rep.str_val = data; }
        }
    } else if (prefix == '*') {
        /* Array replies (KEYS, MGET, …) aren't surfaced in v1.
         * Mark as unsupported; the caller will report it via
         * Redis.LastError(). */
        rep.kind    = 'x';
        rep.str_val = "array replies not supported in v1";
    } else {
        rep.kind    = 'x';
        rep.str_val = "unexpected RESP prefix";
    }
    return rep;
}

/* Send command, read one reply, return true iff the reply is a
 * +SimpleString. Used by SET / EXPIRE / PING etc. — commands whose
 * success is indicated by '+OK' or '+PONG'. */
static inline code_bool _amredis_exec_simple(AmalgameRedis* r, int argc, const char* const* args) {
    if (!r || r->fd < 0) return 0;
    size_t cmd_n = 0;
    char*  cmd   = _amredis_build_cmd(argc, args, &cmd_n);
    if (_amredis_send_all(r->fd, cmd, cmd_n) < 0) {
        r->last_error = _amredis_err_dup("send() failed");
        return 0;
    }
    _AmRedisReply rep = _amredis_read_reply(r->fd);
    if (rep.kind == '+') return 1;
    if (rep.kind == ':') return rep.int_val != 0 ? 1 : 0;
    r->last_error = _amredis_err_dup(
        rep.str_val && *rep.str_val ? rep.str_val : "redis error");
    return 0;
}

/* ── Commands ───────────────────────────────────────── */

/* PING → +PONG. Cheap connection liveness probe. */
static inline code_bool Amalgame_Database_NoSQL_Redis_Ping(AmalgameRedis* r) {
    const char* args[1] = { "PING" };
    return _amredis_exec_simple(r, 1, args);
}

/* SET key value → +OK. Overwrites any existing value, ignores TTL.
 * Use Redis.Expire(key, seconds) afterwards to apply a TTL. */
static inline code_bool Amalgame_Database_NoSQL_Redis_Set(AmalgameRedis* r, code_string key, code_string value) {
    const char* args[3];
    args[0] = "SET";
    args[1] = key   ? key   : "";
    args[2] = value ? value : "";
    return _amredis_exec_simple(r, 3, args);
}

/* GET key → $<len>\r\n<value>\r\n (or $-1\r\n for missing key).
 * Returns the value, or "" both when the key is missing and on
 * error. Use Redis.Exists(key) to disambiguate, or check
 * Redis.LastError() on the latter. */
static inline code_string Amalgame_Database_NoSQL_Redis_Get(AmalgameRedis* r, code_string key) {
    if (!r || r->fd < 0) return "";
    const char* args[2];
    args[0] = "GET";
    args[1] = key ? key : "";
    size_t cmd_n = 0;
    char*  cmd   = _amredis_build_cmd(2, args, &cmd_n);
    if (_amredis_send_all(r->fd, cmd, cmd_n) < 0) {
        r->last_error = _amredis_err_dup("send() failed");
        return "";
    }
    _AmRedisReply rep = _amredis_read_reply(r->fd);
    if (rep.kind == '$') return rep.is_nil ? "" : rep.str_val;
    if (rep.kind == '-' || rep.kind == 'x') {
        r->last_error = _amredis_err_dup(
            rep.str_val && *rep.str_val ? rep.str_val : "redis error");
    }
    return "";
}

/* DEL key → :<count>. Number of keys actually removed (0 or 1 for
 * a single-key DEL). */
static inline i64 Amalgame_Database_NoSQL_Redis_Del(AmalgameRedis* r, code_string key) {
    if (!r || r->fd < 0) return 0;
    const char* args[2];
    args[0] = "DEL";
    args[1] = key ? key : "";
    size_t cmd_n = 0;
    char*  cmd   = _amredis_build_cmd(2, args, &cmd_n);
    if (_amredis_send_all(r->fd, cmd, cmd_n) < 0) {
        r->last_error = _amredis_err_dup("send() failed");
        return 0;
    }
    _AmRedisReply rep = _amredis_read_reply(r->fd);
    if (rep.kind == ':') return rep.int_val;
    if (rep.kind == '-' || rep.kind == 'x') {
        r->last_error = _amredis_err_dup(
            rep.str_val && *rep.str_val ? rep.str_val : "redis error");
    }
    return 0;
}

/* EXISTS key → :1 if present, :0 otherwise. */
static inline code_bool Amalgame_Database_NoSQL_Redis_Exists(AmalgameRedis* r, code_string key) {
    if (!r || r->fd < 0) return 0;
    const char* args[2];
    args[0] = "EXISTS";
    args[1] = key ? key : "";
    size_t cmd_n = 0;
    char*  cmd   = _amredis_build_cmd(2, args, &cmd_n);
    if (_amredis_send_all(r->fd, cmd, cmd_n) < 0) {
        r->last_error = _amredis_err_dup("send() failed");
        return 0;
    }
    _AmRedisReply rep = _amredis_read_reply(r->fd);
    if (rep.kind == ':') return rep.int_val > 0 ? 1 : 0;
    if (rep.kind == '-' || rep.kind == 'x') {
        r->last_error = _amredis_err_dup(
            rep.str_val && *rep.str_val ? rep.str_val : "redis error");
    }
    return 0;
}

/* INCR key → :<new value>. Creates the key with value 1 if it
 * didn't exist; errors against a non-integer value. */
static inline i64 Amalgame_Database_NoSQL_Redis_Incr(AmalgameRedis* r, code_string key) {
    if (!r || r->fd < 0) return 0;
    const char* args[2];
    args[0] = "INCR";
    args[1] = key ? key : "";
    size_t cmd_n = 0;
    char*  cmd   = _amredis_build_cmd(2, args, &cmd_n);
    if (_amredis_send_all(r->fd, cmd, cmd_n) < 0) {
        r->last_error = _amredis_err_dup("send() failed");
        return 0;
    }
    _AmRedisReply rep = _amredis_read_reply(r->fd);
    if (rep.kind == ':') return rep.int_val;
    if (rep.kind == '-' || rep.kind == 'x') {
        r->last_error = _amredis_err_dup(
            rep.str_val && *rep.str_val ? rep.str_val : "redis error");
    }
    return 0;
}

/* DECR key → :<new value>. Mirror of Amalgame_Database_NoSQL_Redis_Incr. */
static inline i64 Amalgame_Database_NoSQL_Redis_Decr(AmalgameRedis* r, code_string key) {
    if (!r || r->fd < 0) return 0;
    const char* args[2];
    args[0] = "DECR";
    args[1] = key ? key : "";
    size_t cmd_n = 0;
    char*  cmd   = _amredis_build_cmd(2, args, &cmd_n);
    if (_amredis_send_all(r->fd, cmd, cmd_n) < 0) {
        r->last_error = _amredis_err_dup("send() failed");
        return 0;
    }
    _AmRedisReply rep = _amredis_read_reply(r->fd);
    if (rep.kind == ':') return rep.int_val;
    if (rep.kind == '-' || rep.kind == 'x') {
        r->last_error = _amredis_err_dup(
            rep.str_val && *rep.str_val ? rep.str_val : "redis error");
    }
    return 0;
}

/* EXPIRE key seconds → :1 on success (TTL set), :0 if the key
 * doesn't exist. Use PEXPIRE for millisecond resolution (v2). */
static inline code_bool Amalgame_Database_NoSQL_Redis_Expire(AmalgameRedis* r, code_string key, i64 seconds) {
    char secStr[32];
    snprintf(secStr, sizeof(secStr), "%lld", (long long) seconds);
    const char* args[3];
    args[0] = "EXPIRE";
    args[1] = key ? key : "";
    args[2] = secStr;
    return _amredis_exec_simple(r, 3, args);
}

/* ═══════════════════════════════════════════════════════
 *  v0.3 — Pub/Sub + Pipelining
 * ═══════════════════════════════════════════════════════
 *
 * Pub/Sub: PUBLISH is fire-and-forget (returns subscriber count).
 * SUBSCRIBE moves the connection into subscriber mode: the server
 * pushes MSG arrays unsolicited. WaitMessage blocks for the next
 * one and stashes channel + payload on the handle.
 *
 *   *3\r\n
 *   $7\r\nmessage\r\n            ← "message" / "subscribe" / "unsubscribe"
 *   $<chan-len>\r\n<chan>\r\n
 *   $<msg-len>\r\n<msg>\r\n
 *
 * Pipelining batches commands client-side: PipelineBegin → many
 * Pipeline* → PipelineExec. The wire writes are buffered and only
 * sent in one chunk at Exec, halving round-trip cost on chains of
 * small commands. Responses are returned in order via
 * PipelineResponseAt(idx).
 */

/* ── RESP array reply parser — needed for pub/sub MSG ─── */

/* Read a single RESP-2 element (bulk string, integer, simple
 * string, or error) and return its payload as a GC-alloc'd C
 * string. Integers and arrays are stringified. Returns NULL on
 * EOF / socket error. */
static inline char* _amredis_read_one_element(int fd) {
    char prefix;
    ssize_t k = recv(fd, &prefix, 1, 0);
    if (k <= 0) return NULL;
    char* line = _amredis_read_line(fd);
    if (!line) return NULL;
    if (prefix == '+' || prefix == '-') {
        return line;
    }
    if (prefix == ':') {
        /* Stringify the integer so the pipeline-response list can
         * hold a uniform List<string>. */
        char tmp[32];
        snprintf(tmp, sizeof(tmp), "%lld", (long long) atoll(line));
        size_t n = strlen(tmp);
        char* p = (char*) code_alloc(n + 1);
        memcpy(p, tmp, n + 1);
        return p;
    }
    if (prefix == '$') {
        long long len = atoll(line);
        if (len < 0) {
            /* nil bulk — represent as empty string in the response list. */
            char* p = (char*) code_alloc(1);
            p[0] = '\0';
            return p;
        }
        return _amredis_read_bulk(fd, (size_t) len);
    }
    /* Arrays inside an element — not used by the v0.3 surface. */
    return NULL;
}

/* Read a multi-bulk array reply. `expected_len` is filled with the
 * declared element count; `out_elements` is a GC-alloc'd array of
 * GC-alloc'd C strings of length expected_len. Returns 0/-1. */
static inline int _amredis_read_array(int fd, long long* expected_len, char*** out_elements) {
    char prefix;
    ssize_t k = recv(fd, &prefix, 1, 0);
    if (k <= 0) return -1;
    if (prefix != '*') return -1;
    char* line = _amredis_read_line(fd);
    if (!line) return -1;
    long long n = atoll(line);
    if (n < 0) { *expected_len = 0; *out_elements = NULL; return 0; }
    char** arr = (char**) code_alloc((size_t) n * sizeof(char*));
    for (long long i = 0; i < n; i++) {
        arr[i] = _amredis_read_one_element(fd);
        if (!arr[i]) return -1;
    }
    *expected_len = n;
    *out_elements = arr;
    return 0;
}

/* ── Pub/Sub ────────────────────────────────────────── */

/* PUBLISH channel message → :N (subscriber count). Returns -1 on
 * error (LastError set), N ≥ 0 on success. Can be called even on
 * a normal (non-subscriber) connection. */
static inline i64 Amalgame_Database_NoSQL_Redis_Publish(
        AmalgameRedis* r, code_string channel, code_string message) {
    if (!r || r->fd < 0) return -1;
    const char* args[3];
    args[0] = "PUBLISH";
    args[1] = channel ? channel : "";
    args[2] = message ? message : "";
    size_t cmd_n = 0;
    char* cmd = _amredis_build_cmd(3, args, &cmd_n);
    if (_amredis_send_all(r->fd, cmd, cmd_n) < 0) {
        r->last_error = _amredis_err_dup("Publish: send failed");
        return -1;
    }
    _AmRedisReply rep = _amredis_read_reply(r->fd);
    if (rep.kind == ':') return rep.int_val;
    r->last_error = _amredis_err_dup(
        rep.str_val && *rep.str_val ? rep.str_val : "PUBLISH unexpected reply");
    return -1;
}

/* SUBSCRIBE channel → *3 [subscribe, channel, sub-count]. Moves
 * the connection into subscriber mode. Successive Subscribe calls
 * stack channels on the same connection. */
static inline code_bool Amalgame_Database_NoSQL_Redis_Subscribe(
        AmalgameRedis* r, code_string channel) {
    if (!r || r->fd < 0 || !channel) return 0;
    const char* args[2];
    args[0] = "SUBSCRIBE";
    args[1] = channel;
    size_t cmd_n = 0;
    char* cmd = _amredis_build_cmd(2, args, &cmd_n);
    if (_amredis_send_all(r->fd, cmd, cmd_n) < 0) {
        r->last_error = _amredis_err_dup("Subscribe: send failed");
        return 0;
    }
    long long n = 0;
    char** elems = NULL;
    if (_amredis_read_array(r->fd, &n, &elems) != 0 || n < 3) {
        r->last_error = _amredis_err_dup("Subscribe: bad confirmation");
        return 0;
    }
    /* Expect elems[0] = "subscribe", elems[1] = channel,
     * elems[2] = stringified subscriber count. */
    if (strcmp(elems[0], "subscribe") != 0) {
        r->last_error = _amredis_err_dup(elems[0] ? elems[0] : "non-subscribe reply");
        return 0;
    }
    r->in_subscriber_mode = 1;
    return 1;
}

/* UNSUBSCRIBE channel → *3 [unsubscribe, channel, remaining]. */
static inline code_bool Amalgame_Database_NoSQL_Redis_Unsubscribe(
        AmalgameRedis* r, code_string channel) {
    if (!r || r->fd < 0 || !channel) return 0;
    const char* args[2];
    args[0] = "UNSUBSCRIBE";
    args[1] = channel;
    size_t cmd_n = 0;
    char* cmd = _amredis_build_cmd(2, args, &cmd_n);
    if (_amredis_send_all(r->fd, cmd, cmd_n) < 0) {
        r->last_error = _amredis_err_dup("Unsubscribe: send failed");
        return 0;
    }
    long long n = 0;
    char** elems = NULL;
    if (_amredis_read_array(r->fd, &n, &elems) != 0 || n < 3) {
        r->last_error = _amredis_err_dup("Unsubscribe: bad confirmation");
        return 0;
    }
    /* Server returns "unsubscribe" + the per-call remaining count
     * (== 0 once we've dropped every channel). */
    if (strcmp(elems[0], "unsubscribe") != 0) {
        r->last_error = _amredis_err_dup(elems[0] ? elems[0] : "non-unsubscribe reply");
        return 0;
    }
    /* Stay in subscriber mode if we still hold other subscriptions;
     * the remaining count is in elems[2]. */
    long long remaining = atoll(elems[2] ? elems[2] : "0");
    if (remaining == 0) r->in_subscriber_mode = 0;
    return 1;
}

/* Block until the next pushed MSG arrives, then stash channel +
 * payload on the handle. timeout_ms is applied to the socket
 * receive — 0 means block forever. Returns 1 on a message, 0 on
 * timeout, -ish on error (LastError set). */
static inline code_bool Amalgame_Database_NoSQL_Redis_WaitMessage(
        AmalgameRedis* r, i64 timeout_ms) {
    if (!r || r->fd < 0) return 0;
    if (!r->in_subscriber_mode) {
        r->last_error = _amredis_err_dup("WaitMessage: not subscribed");
        return 0;
    }
    /* Apply receive timeout. */
#ifdef _WIN32
    DWORD tv = (timeout_ms > 0) ? (DWORD) timeout_ms : 0;
    setsockopt(r->fd, SOL_SOCKET, SO_RCVTIMEO, (const char*) &tv, sizeof(tv));
#else
    struct timeval tv;
    if (timeout_ms > 0) {
        tv.tv_sec  = (time_t) (timeout_ms / 1000);
        tv.tv_usec = (suseconds_t) ((timeout_ms % 1000) * 1000);
    } else {
        tv.tv_sec = 0; tv.tv_usec = 0;
    }
    setsockopt(r->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif

    long long n = 0;
    char** elems = NULL;
    if (_amredis_read_array(r->fd, &n, &elems) != 0 || n < 3) {
        /* Timeout shows up as recv() returning 0 / -1; treat both
         * as "no message" rather than a hard error. */
        return 0;
    }
    /* Expect *3 [message, channel, payload]. */
    if (strcmp(elems[0], "message") != 0) {
        /* Could be a "subscribe" / "unsubscribe" confirmation that
         * leaked through — ignore and report no msg this tick. */
        return 0;
    }
    r->last_channel = elems[1];
    r->last_message = elems[2];
    return 1;
}

static inline code_string Amalgame_Database_NoSQL_Redis_LastChannel(AmalgameRedis* r) {
    if (!r || !r->last_channel) return (code_string) "";
    return (code_string) r->last_channel;
}

static inline code_string Amalgame_Database_NoSQL_Redis_LastMessage(AmalgameRedis* r) {
    if (!r || !r->last_message) return (code_string) "";
    return (code_string) r->last_message;
}

/* ── Pipelining ────────────────────────────────────── */

/* Append `n` bytes to the pipeline_buf, growing if needed. */
static inline void _amredis_pipeline_append(AmalgameRedis* r, const char* bytes, size_t n) {
    size_t need = r->pipeline_buf_n + n;
    if (need > r->pipeline_buf_cap) {
        size_t newcap = r->pipeline_buf_cap ? r->pipeline_buf_cap : 256;
        while (newcap < need) newcap *= 2;
        char* p = (char*) code_alloc(newcap);
        if (r->pipeline_buf_n > 0) memcpy(p, r->pipeline_buf, r->pipeline_buf_n);
        r->pipeline_buf = p;
        r->pipeline_buf_cap = newcap;
    }
    memcpy(r->pipeline_buf + r->pipeline_buf_n, bytes, n);
    r->pipeline_buf_n += n;
}

/* Encode a command into RESP and append it to the pipeline buf.
 * Bumps the queued-command counter. */
static inline void _amredis_pipeline_queue(
        AmalgameRedis* r, int argc, const char* const* args) {
    size_t cmd_n = 0;
    char* cmd = _amredis_build_cmd(argc, args, &cmd_n);
    if (!cmd) return;
    _amredis_pipeline_append(r, cmd, cmd_n);
    r->pipeline_count++;
}

/* Enter pipeline mode: clear the buffer and the counter. */
static inline void Amalgame_Database_NoSQL_Redis_PipelineBegin(AmalgameRedis* r) {
    if (!r) return;
    r->in_pipeline       = 1;
    r->pipeline_buf      = NULL;
    r->pipeline_buf_n    = 0;
    r->pipeline_buf_cap  = 0;
    r->pipeline_count    = 0;
    r->pipeline_responses = NULL;
}

static inline void Amalgame_Database_NoSQL_Redis_PipelineSet(
        AmalgameRedis* r, code_string key, code_string value) {
    if (!r || !r->in_pipeline) return;
    const char* args[3] = { "SET", key ? key : "", value ? value : "" };
    _amredis_pipeline_queue(r, 3, args);
}

static inline void Amalgame_Database_NoSQL_Redis_PipelineGet(
        AmalgameRedis* r, code_string key) {
    if (!r || !r->in_pipeline) return;
    const char* args[2] = { "GET", key ? key : "" };
    _amredis_pipeline_queue(r, 2, args);
}

static inline void Amalgame_Database_NoSQL_Redis_PipelineIncr(
        AmalgameRedis* r, code_string key) {
    if (!r || !r->in_pipeline) return;
    const char* args[2] = { "INCR", key ? key : "" };
    _amredis_pipeline_queue(r, 2, args);
}

static inline void Amalgame_Database_NoSQL_Redis_PipelineDecr(
        AmalgameRedis* r, code_string key) {
    if (!r || !r->in_pipeline) return;
    const char* args[2] = { "DECR", key ? key : "" };
    _amredis_pipeline_queue(r, 2, args);
}

static inline void Amalgame_Database_NoSQL_Redis_PipelineDel(
        AmalgameRedis* r, code_string key) {
    if (!r || !r->in_pipeline) return;
    const char* args[2] = { "DEL", key ? key : "" };
    _amredis_pipeline_queue(r, 2, args);
}

static inline void Amalgame_Database_NoSQL_Redis_PipelineExpire(
        AmalgameRedis* r, code_string key, i64 seconds) {
    if (!r || !r->in_pipeline) return;
    char secStr[24];
    snprintf(secStr, sizeof(secStr), "%lld", (long long) seconds);
    const char* args[3] = { "EXPIRE", key ? key : "", secStr };
    _amredis_pipeline_queue(r, 3, args);
}

/* Flush the buffered commands in one write, then read exactly
 * `pipeline_count` replies in order, stringify each, and store
 * in pipeline_responses. Returns the count of replies read. */
static inline i64 Amalgame_Database_NoSQL_Redis_PipelineExec(AmalgameRedis* r) {
    if (!r || !r->in_pipeline || r->fd < 0) return 0;
    int expected = r->pipeline_count;
    if (expected == 0) {
        r->in_pipeline = 0;
        return 0;
    }
    if (_amredis_send_all(r->fd, r->pipeline_buf, r->pipeline_buf_n) < 0) {
        r->last_error = _amredis_err_dup("PipelineExec: send failed");
        r->in_pipeline = 0;
        return 0;
    }
    AmalgameList* out = AmalgameList_new();
    int got = 0;
    for (int i = 0; i < expected; i++) {
        char* el = _amredis_read_one_element(r->fd);
        if (!el) break;
        AmalgameList_add(out, (void*) el);
        got++;
    }
    r->pipeline_responses = out;
    r->in_pipeline = 0;
    /* Don't clear pipeline_buf — it's GC, drops on its own. */
    return (i64) got;
}

static inline code_string Amalgame_Database_NoSQL_Redis_PipelineResponseAt(
        AmalgameRedis* r, i64 idx) {
    if (!r || !r->pipeline_responses) return (code_string) "";
    if (idx < 0 || idx >= AmalgameList_count(r->pipeline_responses)) {
        return (code_string) "";
    }
    return (code_string) AmalgameList_get(r->pipeline_responses, idx);
}

#endif /* AMALGAME_DATABASE_REDIS_H */
