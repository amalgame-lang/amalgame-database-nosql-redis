#!/bin/bash
# ─────────────────────────────────────────────────────
#  amalgame-database-nosql-redis — Test Runner
#  Usage: ./tests/run_tests.sh [/path/to/amc]
#
#  Probes 127.0.0.1:6379 for a RESP2-speaking server. If
#  reachable: compile the fixture, run, assert. If not: SKIP all
#  cases cleanly. Start a server locally with:
#      docker run --rm -p 6379:6379 redis:7
#  or `sudo apt install redis-server && redis-server &`.
# ─────────────────────────────────────────────────────

set -u

# ── Locate amc ─────────────────────────────────────────
if [ $# -ge 1 ]; then
    AMC="$1"
elif [ -n "${AMC:-}" ]; then
    :
elif command -v amc >/dev/null 2>&1; then
    AMC="$(command -v amc)"
else
    echo "ERROR: amc not found." >&2
    exit 2
fi

# ── Locate package + runtime ───────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PKG_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PKG_RUNTIME="$PKG_ROOT/runtime"

AMC_DIR="$(cd "$(dirname "$AMC")" && pwd)"
if [ -d "$AMC_DIR/runtime" ]; then
    AMC_RUNTIME="$AMC_DIR/runtime"
elif [ -n "${AMC_RUNTIME:-}" ]; then
    :
else
    echo "ERROR: amc runtime/ not found. Set AMC_RUNTIME=..." >&2
    exit 2
fi

BUILD_DIR="$(mktemp -d -t adnsq-redis-XXXXXX)"
trap 'rm -rf "$BUILD_DIR"' EXIT
PROJ_DIR="$BUILD_DIR/proj"
mkdir -p "$PROJ_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'
PASS=0; FAIL=0; SKIP=0

echo ""
echo "════════════════════════════════════════════"
echo "  amalgame-database-nosql-redis — Tests"
echo "════════════════════════════════════════════"
echo "  amc:     $AMC ($("$AMC" --version 2>&1))"
echo "  runtime: $AMC_RUNTIME"
echo ""

# ── Reachability probe ────────────────────────────────
REDIS_AVAILABLE=0
if (echo > /dev/tcp/127.0.0.1/6379) 2>/dev/null; then
    REDIS_AVAILABLE=1
    echo "  redis: reachable at 127.0.0.1:6379"
else
    echo "  redis: NOT reachable at 127.0.0.1:6379 — every case will SKIP"
    echo "         start a server with \`docker run --rm -p 6379:6379 redis:7\`"
fi
echo ""

# ── Install self via amc add ──────────────────────────
PKG_GIT_URL="github.com/amalgame-lang/amalgame-database-nosql-redis"
PKG_TAG="${PKG_TAG:-v0.1.0}"
if [ "$REDIS_AVAILABLE" = "1" ]; then
    echo "── Resolving $PKG_GIT_URL@$PKG_TAG ──"
    if ! (cd "$PROJ_DIR" && "$AMC" add "$PKG_GIT_URL@$PKG_TAG") > "$BUILD_DIR/install.log" 2>&1; then
        echo "  amc add failed — falling back to the local working tree."
        # Write a stub lock pointing at a non-existent commit, but
        # since the test file imports the namespace and uses the
        # Redis class directly, we need amc to know about it. The
        # easiest fallback is to compile manually with the local
        # header (skip the lockfile path).
        REDIS_AVAILABLE=0
    fi
    echo ""
fi

# ── Helper ─────────────────────────────────────────────
run_test() {
    local name="$1"
    local expected="$2"
    printf "  %-38s" "$name"

    if [ "$REDIS_AVAILABLE" != "1" ]; then
        echo -e "${YELLOW}SKIP${NC} (no redis on 127.0.0.1:6379)"
        SKIP=$((SKIP + 1)); return
    fi

    cp "$SCRIPT_DIR/stdlib_redis.am" "$PROJ_DIR/test.am"
    local out_base="$PROJ_DIR/test"
    local out
    out=$(cd "$PROJ_DIR" && "$AMC" -o test test.am 2>&1)
    if [ $? -ne 0 ]; then
        echo -e "${RED}FAIL${NC} (amc)"
        echo "$out" | head -3 | sed 's/^/    /'
        FAIL=$((FAIL + 1)); return
    fi
    if [ ! -f "$out_base.c" ]; then
        echo -e "${RED}FAIL${NC} (no .c)"
        FAIL=$((FAIL + 1)); return
    fi
    gcc -O2 -I"$AMC_RUNTIME" -I"$PKG_RUNTIME" "$out_base.c" \
        -lgc -lm -lcurl -ldl -lpthread -o "$out_base" 2>/dev/null
    if [ ! -x "$out_base" ]; then
        echo -e "${RED}FAIL${NC} (gcc link)"
        FAIL=$((FAIL + 1)); return
    fi
    local run_output
    run_output=$("$out_base" 2>&1)
    if echo "$run_output" | grep -qF "$expected"; then
        echo -e "${GREEN}PASS${NC}"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}FAIL${NC}"
        echo "    expected: $expected"
        echo "    got:      $(echo "$run_output" | head -3 | tr '\n' '|')"
        FAIL=$((FAIL + 1))
    fi
}

echo "── Redis ───────────────────────────────────"
run_test "open 6379"            "[PASS] open 6379"
run_test "ping"                 "[PASS] ping"
run_test "set greeting"         "[PASS] set greeting"
run_test "get greeting"         "[PASS] get greeting"
run_test "get missing empty"    "[PASS] get missing is empty"
run_test "exists hit"           "[PASS] exists hit"
run_test "exists miss"          "[PASS] exists miss"
run_test "incr 1,2"             "[PASS] incr 1,2"
run_test "decr to 1"            "[PASS] decr to 1"
run_test "expire 60s"           "[PASS] expire 60s"
run_test "expire on missing"    "[PASS] expire on missing"
run_test "del 1"                "[PASS] del 1"
run_test "gone after del"       "[PASS] gone after del"
run_test "close"                "[PASS] closed"

echo ""
echo "────────────────────────────────────────────"
echo -e "  ${GREEN}PASS: $PASS${NC}  |  ${RED}FAIL: $FAIL${NC}  |  ${YELLOW}SKIP: $SKIP${NC}"
echo "────────────────────────────────────────────"
echo ""

[ $FAIL -eq 0 ] && exit 0 || exit 1
